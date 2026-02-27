#!/usr/bin/env python3

import sys
import argparse
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

def lookup_upload_id(s3_client, bucket, key):
    """List in-progress multipart uploads for the given key and return the upload ID.
    Exits with an error if zero or more than one upload is found."""
    try:
        uploads = []
        paginator = s3_client.get_paginator('list_multipart_uploads')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=key)
        for page in page_iterator:
            for upload in page.get('Uploads', []):
                if upload['Key'] == key:
                    uploads.append(upload)

        if not uploads:
            print(f"ERROR: No in-progress multipart uploads found for key: {key}")
            sys.exit(1)

        if len(uploads) > 1:
            print(f"ERROR: Multiple in-progress multipart uploads found for key: {key}")
            print("Please abort the unwanted uploads first, or specify the upload ID manually.")
            for u in uploads:
                print(f"  UploadId: {u['UploadId']}  Initiated: {u['Initiated']}")
            sys.exit(1)

        upload_id = uploads[0]['UploadId']
        initiated = str(uploads[0].get('Initiated', 'N/A'))
        print(f"Found upload ID: {upload_id}  Initiated: {initiated}")
        return upload_id, initiated

    except ClientError as e:
        print(f"ERROR listing multipart uploads: {e.response['Error']['Message']}")
        sys.exit(1)


def complete_upload(s3_client, bucket, key, upload_id, initiated='N/A', auto_confirm_after=None):
    try:
        # Step 1: List all parts to get ETags and sizes
        print("Fetching uploaded parts...")
        parts = []
        paginator = s3_client.get_paginator('list_parts')

        page_iterator = paginator.paginate(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )

        for page in page_iterator:
            for part in page['Parts']:
                parts.append({
                    'ETag': part['ETag'],
                    'PartNumber': part['PartNumber'],
                    'Size': part.get('Size', 0),
                    'LastModified': str(part.get('LastModified', 'N/A')),
                })

        if not parts:
            print("ERROR: No parts found for this upload. It might be empty, already aborted, or completed.")
            return False

        # Sort parts by PartNumber to ensure correct order
        parts.sort(key=lambda x: x['PartNumber'])

        total_size = sum(p['Size'] for p in parts)

        # Detect gaps in part numbers
        part_numbers = [p['PartNumber'] for p in parts]
        expected = list(range(part_numbers[0], part_numbers[-1] + 1))
        missing_parts = sorted(set(expected) - set(part_numbers))
        is_continuous = len(missing_parts) == 0

        # Print detailed summary before proceeding
        print(f"\n--- Multipart Upload Summary ---")
        print(f"  Bucket      : {bucket}")
        print(f"  Key         : {key}")
        print(f"  UploadId    : {upload_id}")
        print(f"  Initiated   : {initiated}")
        print(f"  Parts       : {len(parts)}")
        print(f"  Total size  : {total_size:,} bytes ({total_size / (1024 * 1024):.2f} MB)")
        if is_continuous:
            print(f"  Continuity  : OK (parts {part_numbers[0]}-{part_numbers[-1]} are consecutive)")
        else:
            print(f"  Continuity  : WARNING - discontinuous part numbers detected!")
            print(f"  Missing parts: {missing_parts}")
        print(f"\n  {'Part':>6}  {'Size (bytes)':>14}  {'Last Modified':<26}  ETag")
        print(f"  {'-'*6}  {'-'*14}  {'-'*26}  {'-'*36}")
        for p in parts:
            print(f"  {p['PartNumber']:>6}  {p['Size']:>14,}  {p['LastModified']:<26}  {p['ETag']}")
        print()

        # Confirm before completing
        auto_confirmed = False
        if auto_confirm_after is not None and is_continuous:
            try:
                initiated_dt = datetime.fromisoformat(initiated)
                if initiated_dt.tzinfo is None:
                    initiated_dt = initiated_dt.replace(tzinfo=timezone.utc)
                age_seconds = (datetime.now(timezone.utc) - initiated_dt).total_seconds()
                if age_seconds >= auto_confirm_after:
                    print(f"Auto-confirming: upload is {age_seconds:.0f}s old (>= {auto_confirm_after}s threshold) and parts are continuous.")
                    auto_confirmed = True
            except (ValueError, TypeError):
                pass  # Cannot parse initiated time, fall through to manual confirm

        if not auto_confirmed:
            try:
                answer = input("Proceed with complete_multipart_upload? [y/N]: ").strip().lower()
            except EOFError:
                answer = ''
            if answer != 'y':
                print("Aborted by user. No changes were made.")
                return False

        # Step 2: Complete the multipart upload
        response = s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': [{'ETag': p['ETag'], 'PartNumber': p['PartNumber']} for p in parts]}
        )

        print(f"SUCCESS: Upload completed!")
        print(f"   Version ID: {response.get('VersionId', 'N/A')}")
        print(f"   ETag: {response.get('ETag', 'N/A')}")

        # Step 3: Verify the object exists by calling head_object
        print(f"\nVerifying object exists...")
        try:
            head = s3_client.head_object(Bucket=bucket, Key=key)
            print(f"VERIFIED: Object is accessible.")
            print(f"   Content-Length : {head.get('ContentLength', 'N/A'):,} bytes")
            print(f"   ETag           : {head.get('ETag', 'N/A')}")
            print(f"   Last-Modified  : {head.get('LastModified', 'N/A')}")
            print(f"   Version ID     : {head.get('VersionId', 'N/A')}")
        except ClientError as ve:
            print(f"WARNING: complete_multipart_upload succeeded but head_object failed: {ve.response['Error']['Message']}")

        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        
        if error_code == 'InvalidPart':
            print(f"FAILED: InvalidPart error. ETags mismatch or parts missing.")
            print(f"   Details: {error_msg}")
            print("   Suggestion: If parts are missing, you must ABORT this upload.")
        elif error_code == 'EntityTooSmall':
            print(f"FAILED: EntityTooSmall. Total size is less than minimum requirements.")
        elif error_code == 'NoSuchUpload':
            print(f"FAILED: NoSuchUpload. The UploadId is invalid or expired.")
        else:
            print(f"FAILED: AWS Error ({error_code}): {error_msg}")
        return False
    except Exception as e:
        print(f"FAILED: Unexpected error: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Complete a single S3 Multipart Upload. "
                    "The upload ID is resolved automatically by listing in-progress uploads for the given key.")
    parser.add_argument("-b", "--bucket", required=True,
                        help="S3 bucket name.")
    parser.add_argument("-k", "--key", required=True,
                        help="S3 object key of the multipart upload.")
    parser.add_argument("-a", "--auto-confirm-after", required=False, type=int, default=None,
                        dest="auto_confirm_after", metavar="SECONDS",
                        help="If the upload is older than SECONDS and part numbers are "
                             "continuous, skip the confirmation prompt and complete automatically.")

    args = parser.parse_args()

    s3_client = boto3.client('s3')
    upload_id, initiated = lookup_upload_id(s3_client, args.bucket, args.key)
    success = complete_upload(s3_client, args.bucket, args.key, upload_id, initiated, args.auto_confirm_after)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
