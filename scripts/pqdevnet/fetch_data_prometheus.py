#!/usr/bin/env python3
"""
Fetch Lean Consensus metrics from Prometheus and save to Parquet files.

This is a fork-specific script for fetching data from Prometheus instead of ClickHouse.
The upstream project uses fetch_data.py for ClickHouse queries.

Data is organized by devnet iteration (not by date like upstream) since devnets
are ephemeral and may last hours to days.

Usage:
    python fetch_data_prometheus.py --devnet pqdevnet-001   # Fetch specific devnet
    python fetch_data_prometheus.py --devnet all          # Fetch all devnets
    python fetch_data_prometheus.py --list-metrics        # List available lean_* metrics
    python fetch_data_prometheus.py --list-queries        # List available query IDs
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def get_prometheus_client(url: str | None = None) -> PrometheusConnect:
    """Create a Prometheus client."""
    prometheus_url = url or os.environ.get("PROMETHEUS_URL")
    if not prometheus_url:
        raise ValueError("PROMETHEUS_URL environment variable is required")
    return PrometheusConnect(url=prometheus_url, disable_ssl=True)


def load_devnets_manifest(data_dir: Path) -> dict:
    """Load devnets.json manifest."""
    manifest_path = data_dir / "devnets.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Devnets manifest not found at {manifest_path}. "
            "Run 'just detect-devnets' first."
        )
    with open(manifest_path) as f:
        return json.load(f)


def get_devnet_time_range(devnet: dict) -> tuple[datetime, datetime]:
    """Get start/end datetime from a devnet dict."""
    start = datetime.fromisoformat(devnet["start_time"])
    end = datetime.fromisoformat(devnet["end_time"])
    return start, end


# ============================================
# Query Functions
# ============================================
# Each function fetches a specific metric and returns (df, promql_string)
# This mirrors the pattern used in queries/*.py for ClickHouse


def fetch_lean_metrics_overview(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch overview of all lean_* metrics for the devnet period.

    Returns a sample of each metric to verify data availability.
    """
    # Get all metric names starting with lean_
    all_metrics = client.all_metrics()
    lean_metrics = [m for m in all_metrics if m.startswith("lean_")]

    if not lean_metrics:
        return pd.DataFrame(), "# No lean_* metrics found"

    # Fetch a sample from each metric at the end of the devnet
    rows = []
    for metric_name in lean_metrics:
        try:
            result = client.custom_query(query=metric_name, params={"time": end_time.timestamp()})
            rows.append({
                "metric_name": metric_name,
                "series_count": len(result),
                "has_data": len(result) > 0,
            })
        except Exception as e:
            rows.append({
                "metric_name": metric_name,
                "series_count": 0,
                "has_data": False,
                "error": str(e),
            })

    df = pd.DataFrame(rows)
    promql = "# Overview query: checked all lean_* metrics"
    return df, promql


def fetch_head_slot(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch chain head tracking metrics.

    Metrics:
    - lean_head_slot: Latest slot of the lean chain
    - lean_current_slot: Current slot at scrape time
    """
    metrics = ["lean_head_slot", "lean_current_slot"]

    all_rows = []
    for metric in metrics:
        try:
            result = client.custom_query_range(
                query=metric,
                start_time=start_time,
                end_time=end_time,
                step="1m",
            )
            for series in result:
                metric_labels = series.get("metric", {})
                values = series.get("values", [])
                for ts, val in values:
                    row = {
                        "client": metric_labels.get("job", "unknown"),
                        "instance": metric_labels.get("instance", "unknown"),
                        "metric": metric,
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "value": float(val),
                    }
                    all_rows.append(row)
        except Exception:
            pass

    df = pd.DataFrame(all_rows)
    promql = ", ".join(metrics)
    return df, promql


def fetch_fork_choice_reorgs(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch fork choice reorg data.

    Metrics:
    - lean_fork_choice_reorgs_total: Total number of reorgs
    """
    promql = "lean_fork_choice_reorgs_total"
    result = client.custom_query_range(
        query=promql,
        start_time=start_time,
        end_time=end_time,
        step="1m",
    )

    rows = []
    for series in result:
        metric = series.get("metric", {})
        values = series.get("values", [])
        for ts, val in values:
            row = {
                "client": metric.get("job", "unknown"),
                "instance": metric.get("instance", "unknown"),
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                "value": float(val),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    return df, promql


def fetch_finality_metrics(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch finality-related metrics.

    Metrics:
    - lean_latest_justified_slot
    - lean_latest_finalized_slot
    - lean_finalizations_total
    """
    metrics = [
        "lean_latest_justified_slot",
        "lean_latest_finalized_slot",
        "lean_finalized_slot",
        "lean_justified_slot",
    ]

    all_rows = []
    for metric in metrics:
        try:
            result = client.custom_query_range(
                query=metric,
                start_time=start_time,
                end_time=end_time,
                step="1m",
            )
            for series in result:
                metric_labels = series.get("metric", {})
                values = series.get("values", [])
                for ts, val in values:
                    row = {
                        "client": metric_labels.get("job", "unknown"),
                        "instance": metric_labels.get("instance", "unknown"),
                        "metric": metric,
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "value": float(val),
                    }
                    all_rows.append(row)
        except Exception:
            pass

    df = pd.DataFrame(all_rows)
    promql = ", ".join(metrics)
    return df, promql


def fetch_attestation_metrics(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch attestation validation metrics.

    Metrics:
    - lean_attestations_valid_total
    - lean_attestations_invalid_total
    """
    metrics = [
        "lean_attestations_valid_total",
        "lean_attestations_invalid_total",
    ]

    all_rows = []
    for metric in metrics:
        try:
            result = client.custom_query_range(
                query=metric,
                start_time=start_time,
                end_time=end_time,
                step="1m",
            )
            for series in result:
                metric_labels = series.get("metric", {})
                values = series.get("values", [])
                for ts, val in values:
                    row = {
                        "client": metric_labels.get("job", "unknown"),
                        "instance": metric_labels.get("instance", "unknown"),
                        "metric": metric,
                        "source": metric_labels.get("source", "unknown"),
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "value": float(val),
                    }
                    all_rows.append(row)
        except Exception:
            pass

    df = pd.DataFrame(all_rows)
    promql = ", ".join(metrics)
    return df, promql


def fetch_pq_signature_metrics(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch post-quantum signature metrics.

    Metrics:
    - lean_pq_sig_aggregated_signatures_valid_total
    - lean_pq_sig_aggregated_signatures_invalid_total
    - lean_pq_sig_aggregated_signatures_total
    """
    metrics = [
        "lean_pq_sig_aggregated_signatures_valid_total",
        "lean_pq_sig_aggregated_signatures_invalid_total",
        "lean_pq_sig_aggregated_signatures_total",
        "lean_pq_sig_attestations_in_aggregated_signatures_total",
    ]

    all_rows = []
    for metric in metrics:
        try:
            result = client.custom_query_range(
                query=metric,
                start_time=start_time,
                end_time=end_time,
                step="1m",
            )
            for series in result:
                metric_labels = series.get("metric", {})
                values = series.get("values", [])
                for ts, val in values:
                    row = {
                        "client": metric_labels.get("job", "unknown"),
                        "instance": metric_labels.get("instance", "unknown"),
                        "metric": metric,
                        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                        "value": float(val),
                    }
                    all_rows.append(row)
        except Exception:
            pass

    df = pd.DataFrame(all_rows)
    promql = ", ".join(metrics)
    return df, promql


def fetch_pq_signature_timing(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch post-quantum signature timing histograms as percentiles.

    Computes p50, p95, p99 for signing and verification times.
    """
    histogram_metrics = [
        ("lean_pq_signature_attestation_signing_time_seconds", "signing"),
        ("lean_pq_signature_attestation_verification_time_seconds", "verification"),
    ]
    quantiles = [0.5, 0.95, 0.99]

    all_rows = []
    for metric_base, metric_name in histogram_metrics:
        for q in quantiles:
            promql = f'histogram_quantile({q}, rate({metric_base}_bucket[5m]))'
            try:
                result = client.custom_query_range(
                    query=promql,
                    start_time=start_time,
                    end_time=end_time,
                    step="5m",
                )
                for series in result:
                    metric_labels = series.get("metric", {})
                    values = series.get("values", [])
                    for ts, val in values:
                        row = {
                            "client": metric_labels.get("job", "unknown"),
                            "instance": metric_labels.get("instance", "unknown"),
                            "metric": metric_name,
                            "quantile": q,
                            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                            "value": float(val),
                        }
                        all_rows.append(row)
            except Exception:
                pass

    df = pd.DataFrame(all_rows)
    promql = "histogram_quantile(p50/p95/p99, rate(<pq_sig_timing>_bucket[5m]))"
    return df, promql


def fetch_network_peers(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch network peer metrics.

    Metrics:
    - lean_connected_peers
    """
    promql = "lean_connected_peers"
    result = client.custom_query_range(
        query=promql,
        start_time=start_time,
        end_time=end_time,
        step="1m",
    )

    rows = []
    for series in result:
        metric = series.get("metric", {})
        values = series.get("values", [])
        for ts, val in values:
            row = {
                "client": metric.get("job", "unknown"),
                "instance": metric.get("instance", "unknown"),
                "client_type": metric.get("type", "unknown"),
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                "value": float(val),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    return df, promql


def fetch_state_transition_timing(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch state transition timing histograms as percentiles.

    Computes p50, p95, p99 for various state transition operations.
    """
    histogram_metrics = [
        ("lean_state_transition_time_seconds", "total"),
        ("lean_state_transition_slots_processing_time_seconds", "slots"),
        ("lean_state_transition_block_processing_time_seconds", "block"),
        ("lean_state_transition_attestations_processing_time_seconds", "attestations"),
        ("lean_fork_choice_block_processing_time_seconds", "fork_choice"),
        ("lean_attestation_validation_time_seconds", "attestation_validation"),
    ]
    quantiles = [0.5, 0.95, 0.99]

    all_rows = []
    for metric_base, metric_name in histogram_metrics:
        for q in quantiles:
            promql = f'histogram_quantile({q}, rate({metric_base}_bucket[5m]))'
            try:
                result = client.custom_query_range(
                    query=promql,
                    start_time=start_time,
                    end_time=end_time,
                    step="5m",
                )
                for series in result:
                    metric_labels = series.get("metric", {})
                    values = series.get("values", [])
                    for ts, val in values:
                        row = {
                            "client": metric_labels.get("job", "unknown"),
                            "instance": metric_labels.get("instance", "unknown"),
                            "metric": metric_name,
                            "quantile": q,
                            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                            "value": float(val),
                        }
                        all_rows.append(row)
            except Exception:
                pass

    df = pd.DataFrame(all_rows)
    promql = "histogram_quantile(p50/p95/p99, rate(<state_transition_timing>_bucket[5m]))"
    return df, promql


def fetch_validators_count(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> tuple[pd.DataFrame, str]:
    """
    Fetch validator count metrics.

    Metrics:
    - lean_validators_count
    """
    promql = "lean_validators_count"
    result = client.custom_query_range(
        query=promql,
        start_time=start_time,
        end_time=end_time,
        step="1m",
    )

    rows = []
    for series in result:
        metric = series.get("metric", {})
        values = series.get("values", [])
        for ts, val in values:
            row = {
                "client": metric.get("job", "unknown"),
                "instance": metric.get("instance", "unknown"),
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                "value": float(val),
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    return df, promql


# ============================================
# Query Registry
# ============================================

PROMETHEUS_QUERIES = {
    "lean_overview": {
        "function": fetch_lean_metrics_overview,
        "description": "Overview of all lean_* metrics availability",
        "output_file": "lean_overview.parquet",
    },
    "head_slot": {
        "function": fetch_head_slot,
        "description": "Chain head and current slot tracking",
        "output_file": "head_slot.parquet",
    },
    "fork_choice_reorgs": {
        "function": fetch_fork_choice_reorgs,
        "description": "Fork choice reorg counts",
        "output_file": "fork_choice_reorgs.parquet",
    },
    "finality_metrics": {
        "function": fetch_finality_metrics,
        "description": "Justified and finalized slot tracking",
        "output_file": "finality_metrics.parquet",
    },
    "attestation_metrics": {
        "function": fetch_attestation_metrics,
        "description": "Valid/invalid attestation counts",
        "output_file": "attestation_metrics.parquet",
    },
    "pq_signature_metrics": {
        "function": fetch_pq_signature_metrics,
        "description": "Post-quantum signature counts",
        "output_file": "pq_signature_metrics.parquet",
    },
    "pq_signature_timing": {
        "function": fetch_pq_signature_timing,
        "description": "Post-quantum signature timing percentiles",
        "output_file": "pq_signature_timing.parquet",
    },
    "network_peers": {
        "function": fetch_network_peers,
        "description": "Connected peers over time",
        "output_file": "network_peers.parquet",
    },
    "state_transition_timing": {
        "function": fetch_state_transition_timing,
        "description": "State transition timing percentiles",
        "output_file": "state_transition_timing.parquet",
    },
    "validators_count": {
        "function": fetch_validators_count,
        "description": "Number of validators per node",
        "output_file": "validators_count.parquet",
    },
}


def fetch_query(
    client: PrometheusConnect,
    query_id: str,
    query_config: dict,
    start_time: datetime,
    end_time: datetime,
    output_dir: Path,
) -> dict:
    """
    Fetch a single query and return metadata.

    Returns dict with fetched_at, row_count, file_size_bytes.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / query_config["output_file"]

    fetcher = query_config["function"]
    df, promql = fetcher(client, start_time, end_time)

    # Convert to table and add PromQL metadata
    table = pa.Table.from_pandas(df, preserve_index=False)
    existing_metadata = table.schema.metadata or {}
    new_metadata = {**existing_metadata, b"promql": promql.encode("utf-8")}
    table = table.replace_schema_metadata(new_metadata)

    pq.write_table(table, output_path)

    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(df),
        "file_size_bytes": output_path.stat().st_size if output_path.exists() else 0,
    }


def fetch_devnet(
    client: PrometheusConnect,
    devnet: dict,
    output_dir: Path,
    queries_to_run: dict,
) -> dict:
    """Fetch all queries for a devnet iteration."""
    devnet_id = devnet["id"]
    start_time, end_time = get_devnet_time_range(devnet)
    devnet_dir = output_dir / devnet_id

    print(f"\n{devnet_id}: {devnet['duration_hours']}h ({start_time.date()} to {end_time.date()})")

    results = {}
    for query_id, query_config in queries_to_run.items():
        print(f"  Fetching {query_id}...")
        try:
            metadata = fetch_query(
                client, query_id, query_config, start_time, end_time, devnet_dir
            )
            results[query_id] = metadata
            print(f"    -> {metadata['row_count']} rows")
        except Exception as e:
            print(f"    -> ERROR: {e}")

    return results


def update_manifest(
    output_dir: Path,
    devnet_results: dict[str, dict],
) -> None:
    """Update data manifest with fetch results."""
    manifest_path = output_dir / "manifest.json"

    # Load existing or create new
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
    else:
        manifest = {}

    # Ensure required keys exist (handles old date-based manifests)
    manifest.setdefault("schema_version", "1.0")
    manifest.setdefault("source", "prometheus")
    manifest.setdefault("devnets", [])
    manifest.setdefault("devnet_queries", {})

    # Update devnet_queries with new results
    for devnet_id, queries in devnet_results.items():
        if devnet_id not in manifest["devnet_queries"]:
            manifest["devnet_queries"][devnet_id] = {}
        manifest["devnet_queries"][devnet_id].update(queries)

    # Find all devnet directories
    all_devnets = set()
    for d in output_dir.iterdir():
        if d.is_dir() and d.name.startswith("pqdevnet-"):
            all_devnets.add(d.name)
    manifest["devnets"] = sorted(all_devnets)

    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch data from Prometheus")
    parser.add_argument(
        "--devnet",
        help="Devnet ID to fetch (e.g., pqdevnet-001) or 'all' for all devnets",
    )
    parser.add_argument("--output-dir", default="notebooks/data")
    parser.add_argument("--prometheus-url", help="Prometheus URL (or set PROMETHEUS_URL env)")
    parser.add_argument("--query", help="Fetch specific query only")
    parser.add_argument(
        "--list-metrics",
        action="store_true",
        help="List available lean_* metrics and exit",
    )
    parser.add_argument(
        "--list-queries",
        action="store_true",
        help="List available query IDs and exit",
    )
    parser.add_argument(
        "--list-devnets",
        action="store_true",
        help="List detected devnets and exit",
    )
    args = parser.parse_args()

    load_dotenv()
    output_dir = Path(args.output_dir)

    # List queries mode
    if args.list_queries:
        print("Available queries:\n")
        for query_id, config in PROMETHEUS_QUERIES.items():
            print(f"  {query_id}")
            print(f"    {config['description']}")
            print(f"    -> {config['output_file']}")
            print()
        return

    # Create Prometheus client
    client = get_prometheus_client(args.prometheus_url)

    # List metrics mode
    if args.list_metrics:
        print(f"Connecting to Prometheus at {client.url}...")
        try:
            all_metrics = client.all_metrics()
            lean_metrics = sorted([m for m in all_metrics if m.startswith("lean_")])
            if lean_metrics:
                print(f"\nFound {len(lean_metrics)} lean_* metrics:\n")
                for m in lean_metrics:
                    print(f"  {m}")
            else:
                print("\nNo lean_* metrics found.")
                print("\nAll available metrics (first 50):")
                for m in sorted(all_metrics)[:50]:
                    print(f"  {m}")
                if len(all_metrics) > 50:
                    print(f"  ... and {len(all_metrics) - 50} more")
        except Exception as e:
            print(f"Error connecting to Prometheus: {e}")
            sys.exit(1)
        return

    # List devnets mode
    if args.list_devnets:
        try:
            devnets_manifest = load_devnets_manifest(output_dir)
            devnets = devnets_manifest.get("devnets", [])
            if not devnets:
                print("No devnets found. Run 'just detect-devnets' first.")
                return
            print(f"Found {len(devnets)} devnet(s):\n")
            for d in devnets:
                print(f"  {d['id']}: {d['duration_hours']}h")
                print(f"    {d['start_time']} to {d['end_time']}")
                print(f"    Slots: {d['start_slot']} -> {d['end_slot']}")
                print(f"    Clients: {', '.join(d['clients'])}")
                print()
        except FileNotFoundError as e:
            print(e)
            sys.exit(1)
        return

    # Require --devnet for fetching
    if not args.devnet:
        print("Error: --devnet is required. Use --devnet <id> or --devnet all")
        print("Run with --list-devnets to see available devnets.")
        sys.exit(1)

    # Load devnets manifest
    try:
        devnets_manifest = load_devnets_manifest(output_dir)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    devnets = devnets_manifest.get("devnets", [])
    if not devnets:
        print("No devnets found. Run 'just detect-devnets' first.")
        sys.exit(1)

    # Determine which devnets to fetch
    if args.devnet == "all":
        devnets_to_fetch = devnets
    else:
        devnets_to_fetch = [d for d in devnets if d["id"] == args.devnet]
        if not devnets_to_fetch:
            print(f"Devnet '{args.devnet}' not found.")
            print(f"Available devnets: {', '.join(d['id'] for d in devnets)}")
            sys.exit(1)

    # Determine which queries to run
    queries_to_run = PROMETHEUS_QUERIES
    if args.query:
        if args.query not in PROMETHEUS_QUERIES:
            print(f"Unknown query: {args.query}")
            print(f"Available queries: {', '.join(PROMETHEUS_QUERIES.keys())}")
            sys.exit(1)
        queries_to_run = {args.query: PROMETHEUS_QUERIES[args.query]}

    print(f"Prometheus URL: {client.url}")
    print(f"Fetching {len(devnets_to_fetch)} devnet(s), {len(queries_to_run)} query(s) each")

    # Fetch data for each devnet
    all_results = {}
    for devnet in devnets_to_fetch:
        results = fetch_devnet(client, devnet, output_dir, queries_to_run)
        all_results[devnet["id"]] = results

    # Update manifest
    print("\nUpdating manifest...")
    update_manifest(output_dir, all_results)

    print("\nDone!")


if __name__ == "__main__":
    main()
