import os
import sys
import boto3
import botocore
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse


def upload_file(s3, bucket_name, file_path, s3_key):
    """Helper function to check if a file exists in S3 before uploading."""
    try:
        # Check if the file exists in the bucket
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            return  # Skip upload if the file exists
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # File does not exist, proceed with upload
                pass
            else:
                # Handle other errors
                print(f"Error checking file in S3: {e}")
                return

        # Upload the file
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}: {e}")


def upload_to_s3(worker_dir, bucket_name, max_threads=8):
    # Load environment variables from .env
    # load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        print("AWS credentials not found in .env file.")
        sys.exit(1)

    # Initialize S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )

    # Ensure the bucket exists, or create it if it doesn't
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3.create_bucket(Bucket=bucket_name)
        else:
            print(f"Error checking bucket: {e}")
            sys.exit(1)

    # Directory to upload
    crawl_dir = os.path.join(worker_dir, "pt-crawl")
    if not os.path.isdir(crawl_dir):
        print(f"The directory '{crawl_dir}' does not exist.")
        sys.exit(1)

    # Collect all files to upload
    files_to_upload = []
    for root, dirs, files in os.walk(crawl_dir):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = os.path.relpath(
                file_path, crawl_dir
            )  # Relative path within S3 bucket
            files_to_upload.append((file_path, s3_key))

    print(f"Found {len(files_to_upload)} files to upload.")

    # Upload files using multithreading
    with ThreadPoolExecutor(max_threads) as executor:
        futures = []
        for file_path, s3_key in files_to_upload:
            futures.append(
                executor.submit(upload_file, s3, bucket_name, file_path, s3_key)
            )

        print("Uploading batch of " + str(len(futures)) + " files...")
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error during file upload: {e}")

    print("All uploads completed.")


def download_file(s3, bucket_name, s3_key, target_path):
    """Helper function to download a single file."""
    try:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        s3.download_file(bucket_name, s3_key, target_path)
        print(f"Downloaded s3://{bucket_name}/{s3_key} to {target_path}")
    except Exception as e:
        print(f"Failed to download s3://{bucket_name}/{s3_key}: {e}")


def download_from_s3(
    worker_dir, bucket_name, max_results=None, max_threads=8, start=None, end=None
):
    # Load environment variables from .env
    # load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        print("AWS credentials not found in .env file.")
        sys.exit(1)

    # Initialize S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )

    # Check if the bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except botocore.exceptions.ClientError as e:
        print(f"Error: Bucket '{bucket_name}' does not exist or is inaccessible.")
        sys.exit(1)

    # Define the target directory for downloaded data
    download_dir = os.path.join(worker_dir, "pt-crawl")
    os.makedirs(download_dir, exist_ok=True)

    # List all objects in the bucket
    all_files = []

    # Sort files by key
    all_files.sort(key=lambda obj: obj["Key"])

    # Apply start and end range
    if start is not None and end is not None:
        all_files = all_files[start - 1 : end]

    # Download files using multithreading
    def download_file(obj):
        s3_key = obj["Key"]
        file_path = os.path.join(download_dir, os.path.basename(s3_key))
        try:
            s3.download_file(bucket_name, s3_key, file_path)
            print(f"Downloaded {s3_key} to {file_path}")
        except Exception as e:
            print(f"Failed to download s3://{bucket_name}/{s3_key}: {e}")

    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(download_file, obj) for obj in all_files]
        for future in as_completed(futures):
            future.result()

    print("All downloads completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload or download files to/from S3.")
    parser.add_argument(
        "-d", "--download", action="store_true", help="Download files from S3."
    )
    parser.add_argument(
        "-u", "--upload", action="store_true", help="Upload files to S3."
    )
    parser.add_argument(
        "worker_directory", type=str, help="Path to the worker directory."
    )
    parser.add_argument(
        "--max_results",
        type=int,
        default=None,
        help="Max results to download (optional).",
    )
    parser.add_argument(
        "--max_threads",
        type=int,
        default=8,
        help="Number of threads for upload/download (optional).",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=None,
        help="Number of threads for upload/download (optional).",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="Number of threads for upload/download (optional).",
    )

    args = parser.parse_args()

    bucket_name = "pt-crawl"

    if args.upload:
        upload_to_s3(args.worker_directory, bucket_name, args.max_threads)
    elif args.download:
        download_from_s3(
            args.worker_directory,
            bucket_name,
            args.max_results,
            args.max_threads,
            args.start,
            args.end,
        )
    else:
        print("Error: You must specify either --upload or --download.")
        sys.exit(1)
