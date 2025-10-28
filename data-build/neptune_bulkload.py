#!/usr/bin/env python3
"""
Neptune Bulk Loader

Uploads nodes.csv and edges.csv to S3 and triggers Neptune bulk load.
Requires:
- AWS credentials configured (via environment or ~/.aws/credentials)
- S3 bucket with Neptune IAM role access
- Neptune cluster endpoint
"""

import boto3
import json
import time
import os
from pathlib import Path
from typing import Dict, Optional
import argparse


class NeptuneBulkLoader:
    """Handles uploading CSVs to S3 and triggering Neptune bulk load."""

    def __init__(
        self,
        neptune_endpoint: str,
        s3_bucket: str,
        iam_role_arn: str,
        region: str = "us-east-1"
    ):
        """
        Initialize Neptune bulk loader.

        Args:
            neptune_endpoint: Neptune cluster endpoint (e.g., my-cluster.cluster-xxx.us-east-1.neptune.amazonaws.com)
            s3_bucket: S3 bucket name for bulk load files
            iam_role_arn: IAM role ARN with Neptune bulk load permissions
            region: AWS region
        """
        self.neptune_endpoint = neptune_endpoint
        self.s3_bucket = s3_bucket
        self.iam_role_arn = iam_role_arn
        self.region = region

        self.s3_client = boto3.client('s3', region_name=region)
        self.neptune_client = boto3.client('neptunedata', region_name=region)

    def upload_to_s3(self, local_file: Path, s3_key: str) -> str:
        """
        Upload a file to S3.

        Args:
            local_file: Path to local file
            s3_key: S3 object key (path within bucket)

        Returns:
            S3 URI (s3://bucket/key)
        """
        print(f"Uploading {local_file} to s3://{self.s3_bucket}/{s3_key}")

        self.s3_client.upload_file(
            str(local_file),
            self.s3_bucket,
            s3_key
        )

        s3_uri = f"s3://{self.s3_bucket}/{s3_key}"
        print(f"✓ Uploaded to {s3_uri}")
        return s3_uri

    def start_bulk_load(
        self,
        source_s3_uri: str,
        format: str = "csv",
        fail_on_error: bool = False,
        parallelism: str = "MEDIUM"
    ) -> str:
        """
        Start a Neptune bulk load job.

        Args:
            source_s3_uri: S3 URI to load from (can be a directory or file)
            format: Data format (csv, opencypher, ntriples, nquads, rdfxml, turtle)
            fail_on_error: Whether to fail the entire load on any error
            parallelism: Load parallelism (LOW, MEDIUM, HIGH, OVERSUBSCRIBE)

        Returns:
            Load ID
        """
        print(f"\nStarting Neptune bulk load from {source_s3_uri}")

        # Use the Neptune data API endpoint
        neptune_data_endpoint = f"https://{self.neptune_endpoint}:8182"

        response = self.neptune_client.start_loader_job(
            source=source_s3_uri,
            format=format,
            s3BucketRegion=self.region,
            iamRoleArn=self.iam_role_arn,
            mode='AUTO',  # AUTO mode handles both vertices and edges
            failOnError=fail_on_error,
            parallelism=parallelism
        )

        load_id = response['payload']['loadId']
        print(f"✓ Bulk load started with ID: {load_id}")
        return load_id

    def check_load_status(self, load_id: str) -> Dict:
        """
        Check the status of a bulk load job.

        Args:
            load_id: Load job ID

        Returns:
            Status dictionary
        """
        response = self.neptune_client.get_loader_job_status(
            loadId=load_id
        )

        return response['payload']

    def wait_for_load(self, load_id: str, poll_interval: int = 10) -> bool:
        """
        Wait for a bulk load job to complete.

        Args:
            load_id: Load job ID
            poll_interval: Seconds between status checks

        Returns:
            True if successful, False if failed
        """
        print(f"\nWaiting for load {load_id} to complete...")

        while True:
            status = self.check_load_status(load_id)
            overall_status = status['overallStatus']['status']

            print(f"Status: {overall_status}")

            if overall_status == 'LOAD_COMPLETED':
                print("\n✓ Bulk load completed successfully!")
                print(f"Total records: {status['overallStatus']['totalRecords']}")
                return True
            elif overall_status in ['LOAD_FAILED', 'LOAD_CANCELLED']:
                print(f"\n✗ Bulk load failed with status: {overall_status}")
                if 'errors' in status:
                    print("Errors:")
                    print(json.dumps(status['errors'], indent=2))
                return False

            time.sleep(poll_interval)

    def load_graph_data(self, data_dir: Path, wait: bool = True) -> Optional[str]:
        """
        Upload nodes and edges CSV files and start bulk load.

        Args:
            data_dir: Directory containing nodes.csv and edges.csv
            wait: Whether to wait for load to complete

        Returns:
            Load ID if successful, None otherwise
        """
        nodes_file = data_dir / "nodes.csv"
        edges_file = data_dir / "edges.csv"

        # Validate files exist
        if not nodes_file.exists():
            raise FileNotFoundError(f"nodes.csv not found at {nodes_file}")
        if not edges_file.exists():
            raise FileNotFoundError(f"edges.csv not found at {edges_file}")

        # Create timestamped folder in S3
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        s3_prefix = f"neptune_load/{timestamp}"

        # Upload files
        nodes_uri = self.upload_to_s3(nodes_file, f"{s3_prefix}/nodes.csv")
        edges_uri = self.upload_to_s3(edges_file, f"{s3_prefix}/edges.csv")

        # Start bulk load (use directory path, Neptune will load all CSVs)
        s3_load_uri = f"s3://{self.s3_bucket}/{s3_prefix}/"
        load_id = self.start_bulk_load(s3_load_uri)

        if wait:
            success = self.wait_for_load(load_id)
            return load_id if success else None

        return load_id


def main():
    """CLI interface for Neptune bulk loader."""
    parser = argparse.ArgumentParser(
        description="Upload graph data to Neptune via S3 bulk load"
    )
    parser.add_argument(
        "--neptune-endpoint",
        required=False,
        help="Neptune cluster endpoint (or set NEPTUNE_ENDPOINT env var)"
    )
    parser.add_argument(
        "--s3-bucket",
        required=False,
        help="S3 bucket for bulk load files (or set NEPTUNE_S3_BUCKET env var)"
    )
    parser.add_argument(
        "--iam-role-arn",
        required=False,
        help="IAM role ARN with Neptune load permissions (or set NEPTUNE_IAM_ROLE env var)"
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)"
    )
    parser.add_argument(
        "--data-dir",
        default="./out/neptune_load",
        help="Directory containing nodes.csv and edges.csv"
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for load to complete"
    )

    args = parser.parse_args()

    # Get config from args or environment
    neptune_endpoint = args.neptune_endpoint or os.getenv("NEPTUNE_ENDPOINT")
    s3_bucket = args.s3_bucket or os.getenv("NEPTUNE_S3_BUCKET")
    iam_role_arn = args.iam_role_arn or os.getenv("NEPTUNE_IAM_ROLE")

    if not all([neptune_endpoint, s3_bucket, iam_role_arn]):
        print("Error: Missing required configuration. Provide either:")
        print("  1. Command-line arguments: --neptune-endpoint, --s3-bucket, --iam-role-arn")
        print("  2. Environment variables: NEPTUNE_ENDPOINT, NEPTUNE_S3_BUCKET, NEPTUNE_IAM_ROLE")
        return 1

    try:
        loader = NeptuneBulkLoader(
            neptune_endpoint=neptune_endpoint,
            s3_bucket=s3_bucket,
            iam_role_arn=iam_role_arn,
            region=args.region
        )

        data_dir = Path(args.data_dir)
        load_id = loader.load_graph_data(data_dir, wait=not args.no_wait)

        if load_id:
            print(f"\n✓ Load completed with ID: {load_id}")
            return 0
        else:
            print("\n✗ Load failed")
            return 1

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
