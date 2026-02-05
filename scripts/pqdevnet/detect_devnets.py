#!/usr/bin/env python3
"""
Detect Lean Consensus devnet iterations from Prometheus metrics.

Devnets are ephemeral - they start, run for hours/days, then stop.
This script detects iteration boundaries by finding slot resets across
multiple client instances (jobs), clustering them within a tolerance window.

Multiple clients run in each devnet (zeam, qlean, ream, lantern, etc.)
and may restart at slightly different times. A devnet boundary is detected
when multiple clients reset their slots within a short time window.

Usage:
    python detect_devnets.py                    # Detect from last 7 days
    python detect_devnets.py --days 30          # Detect from last 30 days
    python detect_devnets.py --start 2026-01-01 --end 2026-02-01
    python detect_devnets.py --output devnets.json
"""

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class DevnetIteration:
    """Represents a single devnet iteration."""

    id: str
    start_time: str  # ISO format
    end_time: str  # ISO format
    duration_hours: float
    start_slot: int
    end_slot: int
    clients: list[str]  # List of client/job names seen
    notes: str = ""


def devnet_id_from_timestamp(dt: datetime) -> str:
    """Derive a deterministic devnet ID from its start timestamp."""
    return f"pqdevnet-{dt.strftime('%Y%m%dT%H%MZ')}"


# Infrastructure containers irrelevant to devnet client analysis
EXCLUDED_CONTAINERS = {
    "unknown", "cadvisor", "prometheus", "promtail",
    "node-exporter", "node_exporter", "grafana",
}


def get_prometheus_client(url: str | None = None) -> PrometheusConnect:
    """Create a Prometheus client."""
    prometheus_url = url or os.environ.get("PROMETHEUS_URL")
    if not prometheus_url:
        raise ValueError("PROMETHEUS_URL environment variable is required")
    return PrometheusConnect(url=prometheus_url, disable_ssl=True)


def fetch_head_slot_history(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
    step: str = "1m",
) -> pd.DataFrame:
    """Fetch lean_head_slot history for all clients."""
    result = client.custom_query_range(
        query="lean_head_slot",
        start_time=start_time,
        end_time=end_time,
        step=step,
    )

    rows = []
    for series in result:
        metric = series.get("metric", {})
        job = metric.get("job", "unknown")  # Client name
        instance = metric.get("instance", "unknown")
        values = series.get("values", [])
        for ts, val in values:
            rows.append({
                "client": job,
                "instance": instance,
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                "slot": int(float(val)),
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["client", "timestamp"])
    return df


def detect_slot_resets_per_client(
    df: pd.DataFrame,
    reset_threshold: int = 100,
) -> pd.DataFrame:
    """
    Detect slot resets for each client.

    A reset is detected when the slot decreases by more than reset_threshold.
    This handles both full resets (to 0) and partial resets.
    """
    resets = []

    for client in df["client"].unique():
        client_df = df[df["client"] == client].sort_values("timestamp")

        prev_slot = None
        prev_timestamp = None
        for _, row in client_df.iterrows():
            if prev_slot is not None:
                # Detect significant slot decrease (reset)
                if row["slot"] < prev_slot - reset_threshold:
                    resets.append({
                        "client": client,
                        "timestamp": row["timestamp"],
                        "new_slot": row["slot"],
                        "prev_slot": prev_slot,
                        "prev_timestamp": prev_timestamp,
                    })
            prev_slot = row["slot"]
            prev_timestamp = row["timestamp"]

    return pd.DataFrame(resets)


def cluster_resets_across_clients(
    resets_df: pd.DataFrame,
    tolerance_minutes: int = 10,
    min_clients: int = 2,
) -> list[dict]:
    """
    Cluster resets that happen across multiple clients within a tolerance window.

    A cluster is considered a devnet boundary if at least min_clients reset
    within the tolerance window.
    """
    if resets_df.empty:
        return []

    resets_df = resets_df.sort_values("timestamp")
    tolerance = timedelta(minutes=tolerance_minutes)

    clusters = []
    current_cluster = {
        "start": resets_df.iloc[0]["timestamp"],
        "end": resets_df.iloc[0]["timestamp"],
        "clients": {resets_df.iloc[0]["client"]},
        "resets": [resets_df.iloc[0].to_dict()],
    }

    for _, row in resets_df.iloc[1:].iterrows():
        # Check if this reset is within tolerance of current cluster
        if row["timestamp"] - current_cluster["end"] <= tolerance:
            current_cluster["clients"].add(row["client"])
            current_cluster["resets"].append(row.to_dict())
            current_cluster["end"] = max(current_cluster["end"], row["timestamp"])
        else:
            # Save current cluster if it has enough clients
            if len(current_cluster["clients"]) >= min_clients:
                clusters.append(current_cluster)
            # Start new cluster
            current_cluster = {
                "start": row["timestamp"],
                "end": row["timestamp"],
                "clients": {row["client"]},
                "resets": [row.to_dict()],
            }

    # Don't forget the last cluster
    if len(current_cluster["clients"]) >= min_clients:
        clusters.append(current_cluster)

    return clusters


def build_devnet_iterations(
    df: pd.DataFrame,
    clusters: list[dict],
) -> list[DevnetIteration]:
    """
    Build devnet iteration objects from reset clusters.

    Each cluster marks the start of a new devnet iteration.
    The previous iteration ends just before each cluster starts.
    """
    if df.empty:
        return []

    iterations = []
    data_start = df["timestamp"].min()
    data_end = df["timestamp"].max()

    # Sort clusters by start time
    clusters = sorted(clusters, key=lambda c: c["start"])

    # Helper to get slot stats for a time period
    def get_period_stats(start: datetime, end: datetime) -> tuple[int, int, list[str]]:
        period_df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]
        if period_df.empty:
            return 0, 0, []
        return (
            int(period_df["slot"].min()),
            int(period_df["slot"].max()),
            list(period_df["client"].unique()),
        )

    # If there's data before the first cluster, that's the first iteration
    if clusters:
        first_cluster_start = clusters[0]["start"]
        if data_start < first_cluster_start - timedelta(minutes=5):
            start_slot, end_slot, clients = get_period_stats(
                data_start, first_cluster_start - timedelta(seconds=1)
            )
            if clients:
                iterations.append(
                    DevnetIteration(
                        id=devnet_id_from_timestamp(data_start),
                        start_time=data_start.isoformat(),
                        end_time=(first_cluster_start - timedelta(seconds=1)).isoformat(),
                        duration_hours=round(
                            (first_cluster_start - data_start).total_seconds() / 3600, 2
                        ),
                        start_slot=start_slot,
                        end_slot=end_slot,
                        clients=clients,
                        notes="Pre-existing devnet (data starts before first detected reset)",
                    )
                )

    # Process each cluster as the start of a new iteration
    for i, cluster in enumerate(clusters):
        iteration_start = cluster["start"]

        # Iteration ends at the next cluster start, or at data end
        if i + 1 < len(clusters):
            iteration_end = clusters[i + 1]["start"] - timedelta(seconds=1)
        else:
            iteration_end = data_end

        start_slot, end_slot, clients = get_period_stats(iteration_start, iteration_end)

        if not clients:
            continue

        iterations.append(
            DevnetIteration(
                id=devnet_id_from_timestamp(iteration_start),
                start_time=iteration_start.isoformat(),
                end_time=iteration_end.isoformat(),
                duration_hours=round(
                    (iteration_end - iteration_start).total_seconds() / 3600, 2
                ),
                start_slot=start_slot,
                end_slot=end_slot,
                clients=clients,
                notes=f"Detected from {len(cluster['clients'])} client resets",
            )
        )

    return iterations


def detect_devnets(
    client: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
    reset_threshold: int = 100,
    tolerance_minutes: int = 10,
    min_clients: int = 2,
) -> list[DevnetIteration]:
    """
    Main detection function.

    1. Fetch head_slot history for all clients
    2. Detect slot resets per client
    3. Cluster resets that happen across multiple clients
    4. Build devnet iteration objects
    """
    print(f"Fetching head_slot data from {start_time.date()} to {end_time.date()}...")
    df = fetch_head_slot_history(client, start_time, end_time)

    if df.empty:
        print("No data found.")
        return []

    clients = df["client"].unique()
    print(f"Found {len(df)} data points across {len(clients)} clients: {', '.join(clients)}")

    print(f"Detecting slot resets per client (threshold: {reset_threshold} slots)...")
    resets_df = detect_slot_resets_per_client(df, reset_threshold)
    print(f"Found {len(resets_df)} slot resets across all clients")

    if not resets_df.empty:
        for client_name in resets_df["client"].unique():
            count = len(resets_df[resets_df["client"] == client_name])
            print(f"  - {client_name}: {count} resets")

    print(f"Clustering resets across clients (tolerance: {tolerance_minutes}min, min_clients: {min_clients})...")
    clusters = cluster_resets_across_clients(resets_df, tolerance_minutes, min_clients)
    print(f"Found {len(clusters)} devnet boundaries (multi-client resets)")

    if not clusters:
        # No multi-client resets found - treat entire period as one devnet
        print("No multi-client resets detected - treating as single devnet iteration")
        return [
            DevnetIteration(
                id=devnet_id_from_timestamp(df["timestamp"].min()),
                start_time=df["timestamp"].min().isoformat(),
                end_time=df["timestamp"].max().isoformat(),
                duration_hours=round(
                    (df["timestamp"].max() - df["timestamp"].min()).total_seconds()
                    / 3600,
                    2,
                ),
                start_slot=int(df["slot"].min()),
                end_slot=int(df["slot"].max()),
                clients=list(df["client"].unique()),
                notes="Single iteration (no multi-client resets detected)",
            )
        ]

    print("Building devnet iterations...")
    iterations = build_devnet_iterations(df, clusters)

    return iterations


def fetch_container_clients(
    prom: PrometheusConnect,
    start_time: datetime,
    end_time: datetime,
) -> set[str]:
    """Fetch client names from cAdvisor containers for a specific time period."""
    result = prom.custom_query_range(
        query="container_cpu_usage_seconds_total",
        start_time=start_time,
        end_time=end_time,
        step="30m",
    )

    containers = set()
    for series in result:
        metric = series.get("metric", {})
        container = metric.get("name", metric.get("container", "unknown"))
        if container and container not in ("", "POD"):
            containers.add(container)

    # Extract client names (e.g., "lantern_0" -> "lantern")
    clients = set()
    for c in containers:
        if c in EXCLUDED_CONTAINERS or "_" not in c:
            continue
        clients.add(c.rsplit("_", 1)[0])
    return clients


def augment_clients_from_containers(
    iterations: list[DevnetIteration],
    prom: PrometheusConnect,
) -> None:
    """Add clients discovered via cAdvisor containers to each devnet iteration."""
    for iteration in iterations:
        start = datetime.fromisoformat(iteration.start_time)
        end = datetime.fromisoformat(iteration.end_time)
        container_clients = fetch_container_clients(prom, start, end)

        merged = sorted(set(iteration.clients) | container_clients)
        if len(merged) > len(iteration.clients):
            added = sorted(container_clients - set(iteration.clients))
            print(f"  {iteration.id}: added {added} from cAdvisor containers")
            iteration.clients = merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect devnet iterations")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="notebooks/data/devnets.json")
    parser.add_argument("--prometheus-url", help="Prometheus URL")
    parser.add_argument(
        "--reset-threshold",
        type=int,
        default=100,
        help="Minimum slot decrease to detect as reset",
    )
    parser.add_argument(
        "--tolerance",
        type=int,
        default=10,
        help="Minutes tolerance for clustering resets across clients",
    )
    parser.add_argument(
        "--min-clients",
        type=int,
        default=2,
        help="Minimum number of clients that must reset to count as devnet boundary",
    )
    parser.add_argument(
        "--min-duration",
        type=int,
        default=0,
        help="Minimum devnet duration in minutes to include (filters out failed/short runs)",
    )
    args = parser.parse_args()

    load_dotenv()

    # Determine time range
    if args.start:
        start_time = datetime.strptime(args.start, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    else:
        start_time = datetime.now(timezone.utc) - timedelta(days=args.days)

    if args.end:
        end_time = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_time = end_time + timedelta(days=1) - timedelta(seconds=1)
    else:
        end_time = datetime.now(timezone.utc)

    client = get_prometheus_client(args.prometheus_url)
    print(f"Prometheus URL: {client.url}")

    iterations = detect_devnets(
        client,
        start_time,
        end_time,
        reset_threshold=args.reset_threshold,
        tolerance_minutes=args.tolerance,
        min_clients=args.min_clients,
    )

    if not iterations:
        print("\nNo devnet iterations found.")
        return

    # Augment client list with all containers visible via cAdvisor
    print("Fetching container data to discover all running clients...")
    augment_clients_from_containers(iterations, client)

    # Filter out short-lived devnets (likely failed runs)
    min_duration_hours = args.min_duration / 60.0
    if args.min_duration > 0:
        original_count = len(iterations)
        iterations = [d for d in iterations if d.duration_hours >= min_duration_hours]
        filtered_count = original_count - len(iterations)
        if filtered_count > 0:
            print(f"Filtered out {filtered_count} devnet(s) shorter than {args.min_duration} minutes")

    if not iterations:
        print("\nNo devnet iterations meet the minimum duration requirement.")
        return

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Detected {len(iterations)} devnet iteration(s):")
    print(f"{'=' * 60}")

    for devnet in iterations:
        print(f"\n{devnet.id}:")
        print(f"  Start: {devnet.start_time}")
        print(f"  End:   {devnet.end_time}")
        print(f"  Duration: {devnet.duration_hours} hours")
        print(f"  Slots: {devnet.start_slot} -> {devnet.end_slot}")
        print(f"  Clients: {', '.join(devnet.clients)}")
        if devnet.notes:
            print(f"  Notes: {devnet.notes}")

    # Save to file
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "detection_params": {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "reset_threshold": args.reset_threshold,
            "tolerance_minutes": args.tolerance,
            "min_clients": args.min_clients,
            "min_duration_minutes": args.min_duration,
        },
        "devnets": [asdict(d) for d in iterations],
    }

    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nSaved to {output_path}")


if __name__ == "__main__":
    main()
