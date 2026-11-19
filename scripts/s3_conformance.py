#!/usr/bin/env python3
"""S3 Conformance and Performance Verification Script for MinIO.

Tests S3 API compatibility required by Spark, DuckDB, and Delta Lake:
1. Path-style addressing
2. Basic CRUD (PUT / GET / HEAD / DELETE)
3. ListObjectsV2 with pagination
4. Multipart upload (>= 32MB)
5. Object overwrite
6. Concurrent multithreaded writes

Reads configuration from src/common/config.py and .env.
Automatically handles host vs docker container endpoint resolution.
"""

from __future__ import annotations

import concurrent.futures
import os
import sys
import time
from pathlib import Path
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# Ensure project root is on sys.path for direct script execution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import load  # noqa: E402


def get_s3_client(endpoint: str, access_key: str, secret_key: str, region: str) -> Any:
    """Create a boto3 S3 client with path-style addressing."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def resolve_endpoint(cfg: Any) -> str:
    """Resolve reachable S3 endpoint (handles host localhost vs docker internal minio:9000)."""
    endpoints_to_try = [
        os.environ.get("S3_ENDPOINT_OVERRIDE"),
        cfg.s3_endpoint_external,
        cfg.s3_endpoint,
        "http://127.0.0.1:9000",
        "http://localhost:9000",
    ]
    for ep in endpoints_to_try:
        if not ep:
            continue
        try:
            client = get_s3_client(ep, cfg.s3_access_key, cfg.s3_secret_key, cfg.s3_region)
            client.list_buckets()
            return ep
        except (EndpointConnectionError, Exception):
            continue

    # Default to config endpoint if none probed
    return cfg.s3_endpoint_external or cfg.s3_endpoint


class S3ConformanceRunner:
    """Runs S3 API conformance suite against MinIO."""

    def __init__(self, endpoint: str, cfg: Any) -> None:
        self.endpoint = endpoint
        self.bucket = cfg.s3_bucket
        self.client = get_s3_client(endpoint, cfg.s3_access_key, cfg.s3_secret_key, cfg.s3_region)
        self.results: list[dict[str, Any]] = []

    def record(self, test_name: str, passed: bool, elapsed_s: float, notes: str) -> None:
        """Record test result."""
        status = "PASS" if passed else "FAIL"
        self.results.append({
            "test": test_name,
            "status": status,
            "elapsed_s": round(elapsed_s, 3),
            "notes": notes,
        })
        icon = "✅" if passed else "❌"
        print(f"  {icon} {test_name:38} | {status:4} | {elapsed_s:.3f}s | {notes}")

    def test_bucket_readiness(self) -> bool:
        """Test 1: Verify target bucket exists or create it."""
        t0 = time.perf_counter()
        try:
            self.client.head_bucket(Bucket=self.bucket)
            self.record("1. Bucket Readiness (head_bucket)", True, time.perf_counter() - t0, f"Bucket '{self.bucket}' ready")
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchBucket"):
                self.client.create_bucket(Bucket=self.bucket)
                self.record("1. Bucket Readiness (create_bucket)", True, time.perf_counter() - t0, f"Bucket '{self.bucket}' created")
                return True
            self.record("1. Bucket Readiness", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def test_basic_crud(self) -> bool:
        """Test 2: Basic CRUD (PUT / GET / HEAD / DELETE) with path-style."""
        t0 = time.perf_counter()
        key = "conformance_test/crud_sample.txt"
        payload = b"Hello MinIO Zero-Copy Lakehouse! " * 32
        try:
            # PUT
            self.client.put_object(Bucket=self.bucket, Key=key, Body=payload)
            # HEAD
            head = self.client.head_object(Bucket=self.bucket, Key=key)
            assert head["ContentLength"] == len(payload)
            # GET
            get_res = self.client.get_object(Bucket=self.bucket, Key=key)
            body = get_res["Body"].read()
            assert body == payload
            # DELETE
            self.client.delete_object(Bucket=self.bucket, Key=key)
            self.record("2. Basic CRUD (PUT/GET/HEAD/DELETE)", True, time.perf_counter() - t0, "Path-style verified")
            return True
        except Exception as e:
            self.record("2. Basic CRUD", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def test_list_objects_v2_pagination(self) -> bool:
        """Test 3: ListObjectsV2 pagination."""
        t0 = time.perf_counter()
        prefix = "conformance_test/pagination/"
        keys = [f"{prefix}item_{i:02d}.dat" for i in range(15)]
        try:
            # Write 15 small objects
            for k in keys:
                self.client.put_object(Bucket=self.bucket, Key=k, Body=b"pagination_test")

            # Paginate with MaxKeys=5
            listed_keys = []
            continuation_token = None
            page_count = 0
            while True:
                kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 5}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                res = self.client.list_objects_v2(**kwargs)
                page_count += 1
                for item in res.get("Contents", []):
                    listed_keys.append(item["Key"])
                if res.get("IsTruncated"):
                    continuation_token = res.get("NextContinuationToken")
                else:
                    break

            assert len(listed_keys) == 15
            assert page_count >= 3

            # Clean up
            delete_objs = [{"Key": k} for k in keys]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_objs})

            self.record("3. ListObjectsV2 Pagination", True, time.perf_counter() - t0, f"15 keys listed in {page_count} pages")
            return True
        except Exception as e:
            self.record("3. ListObjectsV2 Pagination", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def test_multipart_upload_32mb(self) -> bool:
        """Test 4: Multipart upload (>= 32MB payload, 4 parts of 8MB)."""
        t0 = time.perf_counter()
        key = "conformance_test/multipart_32mb.bin"
        part_size = 8 * 1024 * 1024  # 8MB per part
        num_parts = 4
        total_size = part_size * num_parts  # 32MB

        try:
            mpu = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)
            upload_id = mpu["UploadId"]
            parts = []

            for part_num in range(1, num_parts + 1):
                chunk = os.urandom(part_size)
                res = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_num, "ETag": res["ETag"]})

            self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            # Verify size via HEAD
            head = self.client.head_object(Bucket=self.bucket, Key=key)
            assert head["ContentLength"] == total_size

            # Clean up
            self.client.delete_object(Bucket=self.bucket, Key=key)

            mb_speed = (total_size / (1024 * 1024)) / (time.perf_counter() - t0)
            self.record("4. Multipart Upload (32MB, 4 parts)", True, time.perf_counter() - t0, f"32MB throughput ~{mb_speed:.1f} MB/s")
            return True
        except Exception as e:
            self.record("4. Multipart Upload (32MB)", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def test_overwrite_semantics(self) -> bool:
        """Test 5: Overwrite existing object (critical for checkpointing & Delta metadata)."""
        t0 = time.perf_counter()
        key = "conformance_test/overwrite_test.json"
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=b'{"version": 1}')
            self.client.put_object(Bucket=self.bucket, Key=key, Body=b'{"version": 2}')
            res = self.client.get_object(Bucket=self.bucket, Key=key)
            body = res["Body"].read()
            assert body == b'{"version": 2}'
            self.client.delete_object(Bucket=self.bucket, Key=key)
            self.record("5. Object Overwrite Semantics", True, time.perf_counter() - t0, "Immediate consistency verified")
            return True
        except Exception as e:
            self.record("5. Object Overwrite Semantics", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def test_concurrent_writes(self) -> bool:
        """Test 6: Concurrent multithreaded writes (10 workers)."""
        t0 = time.perf_counter()
        prefix = "conformance_test/concurrent/"
        num_workers = 10

        def write_one(worker_id: int) -> bool:
            k = f"{prefix}worker_{worker_id}.dat"
            data = f"worker_{worker_id}_data_{time.time()}".encode()
            self.client.put_object(Bucket=self.bucket, Key=k, Body=data)
            return True

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(write_one, i) for i in range(num_workers)]
                for f in concurrent.futures.as_completed(futures):
                    assert f.result() is True

            # Clean up
            delete_objs = [{"Key": f"{prefix}worker_{i}.dat"} for i in range(num_workers)]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_objs})

            self.record("6. Concurrent Writes (10 threads)", True, time.perf_counter() - t0, "10 concurrent PUTs successful")
            return True
        except Exception as e:
            self.record("6. Concurrent Writes (10 threads)", False, time.perf_counter() - t0, f"Error: {e}")
            return False

    def print_summary(self) -> int:
        """Print markdown summary table and return status code."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        print("\n" + "=" * 80)
        print(f"📊 KẾT QUẢ KIỂM THỬ S3 CONFORMANCE: {passed}/{total} BÀI PASS")
        print(f"   Endpoint kiểm thử: {self.endpoint} | Bucket: {self.bucket}")
        print("=" * 80)
        for r in self.results:
            icon = "PASS" if r["status"] == "PASS" else "FAIL"
            print(f"| {r['test']:38} | {icon:4} | {r['elapsed_s']:6.3f}s | {r['notes']} |")
        print("=" * 80)
        return 0 if passed == total else 1


def main() -> None:
    """Run S3 conformance suite."""
    cfg = load()
    endpoint = resolve_endpoint(cfg)
    print(f"\n🔍 Bắt đầu kiểm tra S3 Conformance tới MinIO ({endpoint})...\n")

    runner = S3ConformanceRunner(endpoint, cfg)
    runner.test_bucket_readiness()
    runner.test_basic_crud()
    runner.test_list_objects_v2_pagination()
    runner.test_multipart_upload_32mb()
    runner.test_overwrite_semantics()
    runner.test_concurrent_writes()

    code = runner.print_summary()
    sys.exit(code)


if __name__ == "__main__":
    main()
