#!/usr/bin/env python3
"""Deterministic Maven Central JAR downloader for the Spark Docker image.

Replaces slow Ivy/POM resolution (`spark-submit --packages`) inside Docker builds:
- Forces IPv4 DNS resolution to avoid WSL2/BuildKit IPv6 connect timeouts.
- Downloads only the 8 pinned JARs missing from apache/spark:3.5.3.
- Verifies Content-Length on every stream and retries dropped transfers.
"""

from __future__ import annotations

import argparse
import http.client
import os
import socket
import time
import urllib.request
from pathlib import Path
from typing import Any

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"
CHUNK_SIZE = 1024 * 1024  # 1 MiB
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SEC = 60


def force_ipv4() -> None:
    """Prefer IPv4 addresses during DNS resolution to avoid IPv6 timeouts."""
    orig_getaddrinfo = socket.getaddrinfo

    def ipv4_getaddrinfo(*args: Any, **kwargs: Any) -> list[Any]:
        results = orig_getaddrinfo(*args, **kwargs)
        ipv4 = [row for row in results if row[0] == socket.AF_INET]
        return ipv4 or results

    socket.getaddrinfo = ipv4_getaddrinfo  # type: ignore[assignment]


def jar_paths_by_group(group: str) -> list[str]:
    """Return Maven Central relative paths for the requested JAR group."""
    spark_ver = os.environ.get("SPARK_VERSION", "3.5.3")
    scala_bin = os.environ.get("SCALA_BINARY_VERSION", "2.12")
    delta_ver = os.environ.get("DELTA_VERSION", "3.2.1")
    hadoop_aws_ver = os.environ.get("HADOOP_AWS_VERSION", "3.3.4")
    aws_sdk_ver = os.environ.get("AWS_SDK_BUNDLE_VERSION", "1.12.262")

    groups: dict[str, list[str]] = {
        "aws": [
            (
                f"com/amazonaws/aws-java-sdk-bundle/{aws_sdk_ver}/"
                f"aws-java-sdk-bundle-{aws_sdk_ver}.jar"
            ),
        ],
        "core": [
            f"io/delta/delta-spark_{scala_bin}/{delta_ver}/delta-spark_{scala_bin}-{delta_ver}.jar",
            f"io/delta/delta-storage/{delta_ver}/delta-storage-{delta_ver}.jar",
            (
                f"org/apache/spark/spark-sql-kafka-0-10_{scala_bin}/{spark_ver}/"
                f"spark-sql-kafka-0-10_{scala_bin}-{spark_ver}.jar"
            ),
            (
                f"org/apache/spark/spark-token-provider-kafka-0-10_{scala_bin}/{spark_ver}/"
                f"spark-token-provider-kafka-0-10_{scala_bin}-{spark_ver}.jar"
            ),
            "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
            "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
            f"org/apache/hadoop/hadoop-aws/{hadoop_aws_ver}/hadoop-aws-{hadoop_aws_ver}.jar",
        ],
    }
    if group not in groups:
        raise ValueError(f"Unknown JAR group: {group!r}")
    return groups[group]


def download_one(url: str, dest: Path) -> None:
    """Download a single JAR atomically with Content-Length check and retries."""
    if dest.exists() and dest.stat().st_size > 0:
        print(f"SKIP {dest.name} (already exists)", flush=True)
        return

    tmp_path = dest.with_suffix(".jar.tmp")
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"GET  {dest.name} (attempt {attempt}/{MAX_ATTEMPTS})...", flush=True)
            req = urllib.request.Request(url, headers={"User-Agent": "cpc-lakehouse-build/1.0"})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                expected = int(resp.headers.get("Content-Length") or 0)
                total = 0
                with tmp_path.open("wb") as out:
                    while True:
                        chunk = resp.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        out.write(chunk)
                        total += len(chunk)
            if expected and total != expected:
                raise OSError(f"Size mismatch: {total} != {expected}")
            os.replace(tmp_path, dest)
            size_label = (
                f"{total / (1024 * 1024):.1f} MB"
                if total >= 1024 * 1024
                else f"{total // 1024} KB"
            )
            print(f"OK   {dest.name} ({size_label})", flush=True)
            return
        except (OSError, http.client.HTTPException) as exc:
            tmp_path.unlink(missing_ok=True)
            print(f"ERR  {dest.name} ({attempt}/{MAX_ATTEMPTS}): {exc}", flush=True)
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(3 * attempt)


def main() -> int:
    """Parse CLI arguments and download the specified JAR group."""
    parser = argparse.ArgumentParser(description="Download pinned Maven Central JARs.")
    parser.add_argument("--group", choices=("aws", "core"), required=True)
    parser.add_argument("--dest", type=Path, default=Path("/opt/spark/jars"))
    args = parser.parse_args()

    force_ipv4()
    args.dest.mkdir(parents=True, exist_ok=True)

    for rel_path in jar_paths_by_group(args.group):
        filename = os.path.basename(rel_path)
        download_one(f"{MAVEN_CENTRAL}/{rel_path}", args.dest / filename)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
