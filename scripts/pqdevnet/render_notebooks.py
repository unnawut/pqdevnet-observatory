#!/usr/bin/env python3
"""
Render Lean Consensus notebooks to HTML.

Executes notebooks with papermill (with devnet_id parameter) and converts to HTML.
This is the fork-specific rendering script for devnet-based notebooks.

Usage:
    python render_notebooks.py --devnet pqdevnet-005
    python render_notebooks.py --devnet all
    python render_notebooks.py --devnet pqdevnet-005 --notebook 01-pq-signature-performance
"""

import argparse
import hashlib
import json
import random
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import nbformat
import papermill as pm
import yaml
from nbconvert import HTMLExporter
from traitlets.config import Config

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

DATA_ROOT = Path("notebooks/data")
OUTPUT_DIR = Path("site/rendered")
MANIFEST_PATH = OUTPUT_DIR / "manifest.json"
TEMPLATE_DIR = Path("notebooks/templates")
LEAN_CONFIG_PATH = Path("pqdevnet-pipeline.yaml")


def load_lean_config() -> dict:
    """Load Lean notebooks configuration."""
    if LEAN_CONFIG_PATH.exists():
        with open(LEAN_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    # Default config if file doesn't exist
    return {
        "notebooks": [
            {
                "id": "lean-01-pq-signature-performance",
                "title": "PQ Signature Performance",
                "icon": "Key",
                "source": "notebooks/lean-01-pq-signature-performance.ipynb",
            }
        ]
    }


def load_devnets() -> list[dict]:
    """Load available devnets from devnets.json."""
    devnets_path = DATA_ROOT / "devnets.json"
    if not devnets_path.exists():
        return []
    with open(devnets_path) as f:
        return json.load(f).get("devnets", [])


def load_manifest() -> dict:
    """Load existing manifest or return empty structure."""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            return json.load(f)
    return {"latest_devnet": "", "devnets": {}, "updated_at": ""}


def save_manifest(manifest: dict) -> None:
    """Save manifest to disk."""
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def hash_file(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def hash_data_dir(devnet_id: str) -> str:
    """Compute combined hash of all parquet files for a devnet."""
    devnet_dir = DATA_ROOT / devnet_id
    if not devnet_dir.exists():
        return ""

    file_hashes = []
    for parquet_file in sorted(devnet_dir.glob("*.parquet")):
        file_hash = hash_file(parquet_file)
        if file_hash:
            file_hashes.append(f"{parquet_file.name}:{file_hash}")

    if not file_hashes:
        return ""

    combined = "|".join(file_hashes)
    return hashlib.sha256(combined.encode()).hexdigest()[:12]


def should_render(
    notebook_id: str,
    notebook_source: Path,
    devnet_id: str,
    manifest: dict,
    force: bool = False,
) -> tuple[bool, str]:
    """Check if a notebook needs to be re-rendered."""
    if force:
        return True, "forced"

    existing = manifest.get("devnets", {}).get(devnet_id, {}).get(notebook_id)
    if not existing:
        return True, "new"

    # Check if notebook source changed
    current_hash = hash_file(notebook_source)
    if current_hash != existing.get("notebook_hash"):
        return True, "notebook changed"

    # Check if data files changed
    current_data_hash = hash_data_dir(devnet_id)
    if current_data_hash and current_data_hash != existing.get("data_hash"):
        return True, "data changed"

    return False, "unchanged"


def inject_plotly_renderer(nb: nbformat.NotebookNode) -> nbformat.NotebookNode:
    """Inject a cell to configure Plotly renderer for HTML export."""
    setup_code = """# Auto-injected: Configure Plotly for HTML export
import plotly.io as pio
pio.renderers.default = "notebook"
"""
    setup_cell = nbformat.v4.new_code_cell(source=setup_code)
    setup_cell.metadata["tags"] = ["setup"]

    # Insert after parameters cell
    insert_idx = 0
    for i, cell in enumerate(nb.cells):
        if cell.cell_type == "code":
            tags = cell.metadata.get("tags", [])
            if "parameters" in tags:
                insert_idx = i + 1
                break

    nb.cells.insert(insert_idx, setup_cell)
    return nb


def render_notebook(
    notebook_id: str,
    notebook_source: Path,
    devnet_id: str,
    output_dir: Path,
) -> tuple[bool, str]:
    """Render a single notebook for a specific devnet using papermill + nbconvert."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{notebook_id}.html"

    abs_source = notebook_source.resolve()
    abs_template_dir = TEMPLATE_DIR.resolve()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            executed_nb = tmp_path / f"{notebook_id}_executed.ipynb"
            prepared_nb = tmp_path / f"{notebook_id}_prepared.ipynb"

            # Read notebook and inject Plotly renderer config
            with open(abs_source) as f:
                nb = nbformat.read(f, as_version=4)
            nb = inject_plotly_renderer(nb)

            # Write prepared notebook
            with open(prepared_nb, "w") as f:
                nbformat.write(nb, f)

            # Execute notebook with papermill
            max_retries = 10
            last_error = None
            for attempt in range(max_retries):
                try:
                    pm.execute_notebook(
                        str(prepared_nb),
                        str(executed_nb),
                        parameters={"devnet_id": devnet_id},  # Use devnet_id instead of target_date
                        cwd=str(abs_source.parent),
                        kernel_name="python3",
                    )
                    break
                except Exception as e:
                    last_error = e
                    error_str = str(e)
                    if attempt < max_retries - 1 and (
                        "ZMQError" in error_str
                        or "Address already in use" in error_str
                        or "Kernel didn't respond" in error_str
                        or "Kernel died" in error_str
                    ):
                        time.sleep(random.uniform(1, 3))
                        continue
                    raise
            else:
                raise last_error

            # Convert to HTML with custom template
            c = Config()
            c.HTMLExporter.extra_template_basedirs = [str(abs_template_dir)]
            c.HTMLExporter.template_name = "minimal"
            c.HTMLExporter.exclude_input_prompt = True
            c.HTMLExporter.exclude_output_prompt = True

            exporter = HTMLExporter(config=c)

            with open(executed_nb) as f:
                nb = nbformat.read(f, as_version=4)

            html_content, resources = exporter.from_notebook_node(nb)

            with open(output_file, "w") as f:
                f.write(html_content)

            # Handle extracted resources
            if resources.get("outputs"):
                files_dir = output_dir / f"{notebook_id}_files"
                files_dir.mkdir(exist_ok=True)
                for filename, data in resources["outputs"].items():
                    with open(files_dir / filename, "wb") as f:
                        f.write(data)

        return True, str(output_file)

    except Exception as e:
        return False, str(e)[:500]


def render_notebook_task(
    notebook_id: str,
    notebook_source_str: str,
    devnet_id: str,
    output_dir_str: str,
) -> dict:
    """Worker function for parallel rendering."""
    notebook_source = Path(notebook_source_str)
    output_dir = Path(output_dir_str)

    ok, result = render_notebook(notebook_id, notebook_source, devnet_id, output_dir)

    return {
        "notebook_id": notebook_id,
        "devnet_id": devnet_id,
        "success": ok,
        "result": result,
        "notebook_hash": hash_file(notebook_source) if ok else "",
        "data_hash": hash_data_dir(devnet_id) if ok else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Render Lean notebooks to HTML")
    parser.add_argument(
        "--devnet",
        required=True,
        help="Devnet ID to render (e.g., pqdevnet-005) or 'all' for all devnets",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Output directory for rendered HTML",
    )
    parser.add_argument(
        "--notebook",
        help="Specific notebook ID to render",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-render even if unchanged",
    )
    parser.add_argument(
        "--list-devnets",
        action="store_true",
        help="List available devnets and exit",
    )
    args = parser.parse_args()

    # List devnets mode
    if args.list_devnets:
        devnets = load_devnets()
        if not devnets:
            print("No devnets found. Run 'just detect-devnets' first.")
            return
        print(f"Available devnets ({len(devnets)}):")
        for d in devnets:
            print(f"  {d['id']}: {d['duration_hours']:.1f}h")
        return

    config = load_lean_config()
    manifest = load_manifest()
    notebooks = config["notebooks"]

    # Load available devnets
    available_devnets = load_devnets()
    if not available_devnets:
        print("No devnets found. Run 'just detect-devnets' first.")
        sys.exit(1)

    available_devnet_ids = [d["id"] for d in available_devnets]

    # Determine devnets to render
    if args.devnet == "all":
        devnets_to_render = available_devnet_ids
    else:
        if args.devnet not in available_devnet_ids:
            print(f"Devnet '{args.devnet}' not found.")
            print(f"Available: {', '.join(available_devnet_ids)}")
            sys.exit(1)
        devnets_to_render = [args.devnet]

    # Check data exists for devnets
    devnets_with_data = []
    for devnet_id in devnets_to_render:
        devnet_dir = DATA_ROOT / devnet_id
        if devnet_dir.exists() and list(devnet_dir.glob("*.parquet")):
            devnets_with_data.append(devnet_id)
        else:
            print(f"WARNING: No data for {devnet_id}, skipping")

    if not devnets_with_data:
        print("No devnets have data. Run 'just fetch-devnet <id>' first.")
        sys.exit(1)

    # Filter notebooks if specified
    if args.notebook:
        notebooks = [nb for nb in notebooks if nb["id"] == args.notebook]
        if not notebooks:
            print(f"Notebook '{args.notebook}' not found in config")
            sys.exit(1)

    latest_devnet = available_devnet_ids[-1] if available_devnet_ids else ""

    print(f"Rendering {len(notebooks)} notebook(s) for {len(devnets_with_data)} devnet(s)")
    print(f"Latest devnet: {latest_devnet}")
    print()

    success_count = 0
    skip_count = 0
    failed = []

    max_workers = min(len(notebooks), 4)

    for devnet_id in devnets_with_data:
        devnet_output_dir = args.output_dir / devnet_id

        if devnet_id not in manifest.get("devnets", {}):
            if "devnets" not in manifest:
                manifest["devnets"] = {}
            manifest["devnets"][devnet_id] = {}

        # Collect notebooks that need rendering
        to_render = []
        for nb in notebooks:
            notebook_id = nb["id"]
            notebook_source = Path(nb["source"])

            needs_render, reason = should_render(
                notebook_id, notebook_source, devnet_id, manifest, args.force
            )
            if not needs_render:
                print(f"  SKIP: {notebook_id} @ {devnet_id} ({reason})")
                skip_count += 1
                continue

            to_render.append((notebook_id, str(notebook_source), reason))

        if not to_render:
            continue

        print(f"  Rendering {len(to_render)} notebook(s) @ {devnet_id}...")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    render_notebook_task,
                    notebook_id,
                    notebook_source_str,
                    devnet_id,
                    str(devnet_output_dir),
                ): (notebook_id, reason)
                for notebook_id, notebook_source_str, reason in to_render
            }

            for future in as_completed(futures):
                result = future.result()
                notebook_id, reason = futures[future]

                if result["success"]:
                    print(f"    {notebook_id}: OK ({reason})")
                    success_count += 1

                    html_path = f"{devnet_id}/{notebook_id}.html"
                    manifest["devnets"][devnet_id][notebook_id] = {
                        "rendered_at": datetime.now(timezone.utc).isoformat(),
                        "notebook_hash": result["notebook_hash"],
                        "data_hash": result["data_hash"],
                        "html_path": html_path,
                    }
                else:
                    print(f"    {notebook_id}: FAILED")
                    failed.append((devnet_id, notebook_id, result["result"]))

    # Update latest devnet
    manifest["latest_devnet"] = latest_devnet

    # Save manifest
    save_manifest(manifest)

    print()
    print(f"Rendered: {success_count}, Skipped: {skip_count}, Failed: {len(failed)}")

    if failed:
        print("\nFailed renders:")
        for devnet_id, notebook_id, err in failed:
            print(f"  {devnet_id}/{notebook_id}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
