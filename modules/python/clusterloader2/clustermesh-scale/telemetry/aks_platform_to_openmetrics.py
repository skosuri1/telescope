#!/usr/bin/env python3
"""Export all AKS Azure Monitor platform metrics as OpenMetrics samples."""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from amw_tsdb_snapshot import (
    escape_label_value,
    format_timestamp_seconds,
    parse_time,
    sanitize_label_name,
    sanitize_metric_name,
)


def run_az(arguments, timeout_seconds):
    result = subprocess.run(
        ["az", *arguments, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    return json.loads(result.stdout)


def metadata_labels(values):
    labels = {}
    for item in values or []:
        name = (
            item.get("name", {}).get("value")
            or item.get("name", {}).get("localizedValue")
            or ""
        )
        if name:
            labels[sanitize_label_name(name)] = item.get("value", "")
    return labels


def render_labels(labels):
    if not labels:
        return ""
    rendered = ",".join(
        f'{name}="{escape_label_value(value)}"'
        for name, value in sorted(labels.items())
    )
    return f"{{{rendered}}}"


def write_text_atomic(path, content):
    """Write text through a same-directory atomic replacement."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def error_detail(error):
    """Return a stable error string for subprocess and decoding failures."""
    detail = getattr(error, "stderr", "") or str(error)
    if isinstance(detail, bytes):
        detail = detail.decode("utf-8", errors="replace")
    return detail.strip()


def effective_command_timeout(command_timeout_seconds, deadline):
    """Cap one Azure CLI command by the remaining exporter deadline."""
    if deadline is None:
        return command_timeout_seconds
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(
            cmd="AKS platform export total deadline",
            timeout=0,
        )
    return min(command_timeout_seconds, remaining)


def manifest_payload(
    args,
    start,
    end,
    definitions,
    exported,
    no_data,
    errors,
    status,
):
    """Build the incrementally checkpointed export manifest."""
    return {
        "schema_version": 1,
        "resource": args.resource,
        "cluster_label": args.cluster_label,
        "start": start,
        "end": end,
        "status": status,
        "complete": status == "complete" and not errors,
        "definitions": definitions,
        "exported": exported,
        "no_data": no_data,
        "errors": errors,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def checkpoint_manifest(
    args,
    start,
    end,
    definitions,
    exported,
    no_data,
    errors,
    status,
):
    """Atomically preserve current export progress."""
    write_text_atomic(
        args.manifest,
        json.dumps(
            manifest_payload(
                args,
                start,
                end,
                definitions,
                exported,
                no_data,
                errors,
                status,
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def export_aggregation(
    output,
    definition,
    aggregation,
    args,
    start,
    end,
    timeout_seconds,
):
    metric_name = definition["name"]["value"]
    value_key = aggregation.lower()
    local_name = sanitize_metric_name(
        f"azure_platform_{metric_name}_{value_key}"
    )
    interval = definition.get("metricAvailabilities", [{}])[0].get(
        "timeGrain",
        "PT1M",
    )
    response = run_az(
        [
            "monitor",
            "metrics",
            "list",
            "--resource",
            args.resource,
            "--metric",
            metric_name,
            "--interval",
            interval,
            "--aggregation",
            aggregation,
            "--start-time",
            start,
            "--end-time",
            end,
        ],
        timeout_seconds,
    )

    sample_count = 0
    output.write(f"# TYPE {local_name} gauge\n")
    for metric in response.get("value", []):
        for series in metric.get("timeseries", []):
            labels = metadata_labels(series.get("metadatavalues"))
            labels.update(
                {
                    "cluster": args.cluster_label,
                    "source": "azure-monitor-platform",
                    "unit": definition.get("unit", ""),
                }
            )
            rendered_labels = render_labels(labels)
            for point in series.get("data", []):
                value = point.get(value_key)
                if value is None:
                    continue
                timestamp = parse_time(point["timeStamp"])
                output.write(
                    f"{local_name}{rendered_labels} {value} "
                    f"{format_timestamp_seconds(timestamp)}\n"
                )
                sample_count += 1
    return local_name, interval, sample_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource", required=True)
    parser.add_argument("--cluster-label", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--command-timeout-seconds", type=int, default=60)
    parser.add_argument("--total-timeout-seconds", type=int, default=0)
    args = parser.parse_args()
    if args.command_timeout_seconds <= 0:
        parser.error("--command-timeout-seconds must be positive")
    if args.total_timeout_seconds < 0:
        parser.error("--total-timeout-seconds must be non-negative")

    start = datetime.fromtimestamp(parse_time(args.start), timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    end = datetime.fromtimestamp(parse_time(args.end), timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported = []
    no_data = []
    errors = []
    definition_count = 0
    deadline = (
        time.monotonic() + args.total_timeout_seconds
        if args.total_timeout_seconds
        else None
    )
    checkpoint_manifest(
        args,
        start,
        end,
        definition_count,
        exported,
        no_data,
        errors,
        "querying-definitions",
    )
    try:
        definitions = run_az(
            [
                "monitor",
                "metrics",
                "list-definitions",
                "--resource",
                args.resource,
            ],
            effective_command_timeout(
                args.command_timeout_seconds,
                deadline,
            ),
        )
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as error:
        errors.append(
            {
                "stage": "list-definitions",
                "error": error_detail(error),
            }
        )
        checkpoint_manifest(
            args,
            start,
            end,
            definition_count,
            exported,
            no_data,
            errors,
            "failed",
        )
        print(
            f"platform metric definition query failed: {error_detail(error)}",
            file=sys.stderr,
        )
        return 1

    definition_count = len(definitions)
    checkpoint_manifest(
        args,
        start,
        end,
        definition_count,
        exported,
        no_data,
        errors,
        "exporting",
    )
    deadline_exhausted = False
    with output_path.open("w", encoding="utf-8") as output:
        for index, definition in enumerate(definitions, start=1):
            metric_name = definition["name"]["value"]
            aggregations = (
                definition.get("supportedAggregationTypes")
                or [definition.get("primaryAggregationType") or "Average"]
            )
            for aggregation in aggregations:
                interval = definition.get("metricAvailabilities", [{}])[0].get(
                    "timeGrain",
                    "PT1M",
                )
                try:
                    command_timeout = effective_command_timeout(
                        args.command_timeout_seconds,
                        deadline,
                    )
                    local_name, interval, sample_count = export_aggregation(
                        output,
                        definition,
                        aggregation,
                        args,
                        start,
                        end,
                        command_timeout,
                    )
                except (
                    subprocess.CalledProcessError,
                    subprocess.TimeoutExpired,
                    json.JSONDecodeError,
                ) as error:
                    detail = error_detail(error)
                    errors.append(
                        {
                            "metric": metric_name,
                            "aggregation": aggregation,
                            "interval": interval,
                            "error": detail,
                        }
                    )
                    print(
                        f"platform metric export failed for "
                        f"{metric_name}:{aggregation}: "
                        f"{detail}",
                        file=sys.stderr,
                        flush=True,
                    )
                    checkpoint_manifest(
                        args,
                        start,
                        end,
                        definition_count,
                        exported,
                        no_data,
                        errors,
                        "exporting",
                    )
                    if (
                        deadline is not None
                        and deadline - time.monotonic() <= 0.1
                    ):
                        deadline_exhausted = True
                        break
                    continue

                key = f"{metric_name}:{aggregation}"
                if sample_count:
                    exported.append(
                        {
                            "source_metric": metric_name,
                            "local_metric": local_name,
                            "aggregation": aggregation,
                            "interval": interval,
                            "samples": sample_count,
                        }
                    )
                else:
                    no_data.append(key)
                output.flush()
                checkpoint_manifest(
                    args,
                    start,
                    end,
                    definition_count,
                    exported,
                    no_data,
                    errors,
                    "exporting",
                )
                print(
                    f"platform metrics {index}/{len(definitions)}: "
                    f"{key} samples={sample_count}",
                    flush=True,
                )
            if deadline_exhausted:
                break
        if not deadline_exhausted:
            output.write("# EOF\n")

    checkpoint_manifest(
        args,
        start,
        end,
        definition_count,
        exported,
        no_data,
        errors,
        "failed" if errors or deadline_exhausted else "complete",
    )
    if errors:
        print(
            f"{len(errors)} platform metric export request(s) failed; "
            f"inspect {args.manifest}.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
