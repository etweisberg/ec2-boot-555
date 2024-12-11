import boto3
import botocore
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Initialize S3 client
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
logging.basicConfig(
    filename="log.txt",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
)


# Use Multi-Threading to Download Files from S3 Bucket pt-counts into ./data/original
def download_files_from_s3_bucket(bucket_name, max_threads=16):
    """
    Downloads all files from an S3 bucket into the local directory ./data/original.
    Handles large buckets by paginating through all objects.
    """
    # Ensure the output directory exists
    os.makedirs("./data/original", exist_ok=True)

    def list_all_files(bucket_name):
        """
        List all files in the S3 bucket using pagination.
        """
        continuation_token = None
        all_files = []
        logging.info(f"Starting to list all files in bucket: {bucket_name}")

        while True:
            try:
                if continuation_token:
                    response = s3.list_objects_v2(
                        Bucket=bucket_name, ContinuationToken=continuation_token
                    )
                else:
                    response = s3.list_objects_v2(Bucket=bucket_name)

                if "Contents" in response:
                    all_files.extend(response["Contents"])
                    logging.info(
                        f"Retrieved {len(response['Contents'])} files, total so far: {len(all_files)}"
                    )
                else:
                    logging.warning("No contents found in the bucket response.")

                # Check if more files are available
                if response.get("IsTruncated"):  # True if there are more objects
                    continuation_token = response["NextContinuationToken"]
                else:
                    break
            except Exception as e:
                logging.error(f"Error while listing files: {e}")
                break

        logging.info(f"Completed listing files. Total files: {len(all_files)}")
        return all_files

    def download_file(file):
        file_name = file["Key"]  # Use the exact S3 key as the filename
        local_path = os.path.join("./data/original", file_name)
        try:
            # Ensure the directory structure for the file exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # Download the file from S3
            s3.download_file(bucket_name, file_name, local_path)
            logging.info(f"Successfully downloaded: {file_name}")
        except Exception as e:
            logging.error(f"Failed to download {file_name}: {e}")

    try:
        # List all files in the bucket
        all_files = list_all_files(bucket_name)

        if not all_files:
            logging.warning(f"No files found in the bucket: {bucket_name}")
            return

        # Use multithreading to download files
        with ThreadPoolExecutor(max_threads) as executor:
            futures = [executor.submit(download_file, file) for file in all_files]
            i = 0
            for future in as_completed(futures):
                future.result()
                i += 1
                logging.info(
                    f"Progress: Downloaded {i}/{len(all_files)} files from {bucket_name}"
                )

        logging.info(
            f"Successfully downloaded all {len(all_files)} files from bucket {bucket_name}"
        )
    except Exception as e:
        logging.error(f"An error occurred during the download process: {e}")


# Go through the ./data/original folder and load each row string into a map of column names to values
def parse_row_to_dict(row):
    """
    Parses a row string into a dictionary with the first token as the key,
    and processes term-frequency pairs and special keys (__url__, __max__).

    Args:
        row (str): The input row as a single string.

    Returns:
        dict: A dictionary with structured data.
    """
    tokens = row.split()
    row_dict = {}

    # The first token is the special "key"
    row_dict["key"] = tokens[0]
    i = 1  # Start processing from the second token

    while i < len(tokens):
        term = tokens[i]
        if term == "__url__":
            # Special case for __url__ (string value)
            row_dict["__url__"] = tokens[i + 2]
            i += 3
        elif term == "__max__":
            # Special case for __max__ (integer value)
            row_dict["__max__"] = int(tokens[i + 2])
            i += 3
        else:
            # General term-frequency pair
            if i + 1 < len(tokens):
                row_dict[term] = int(tokens[i + 2])
                i += 3
            else:
                raise ValueError(f"Malformed row: {row}")

    return row_dict


def load_data_from_files(data_dir, max_threads=16):
    data = []

    def load_file(file_path):
        with open(file_path, "r") as f:
            for row in f:
                data.append(parse_row_to_dict(row))

    files = [
        os.path.join(data_dir, file)
        for file in os.listdir(data_dir)
        if os.path.isfile(os.path.join(data_dir, file))
    ]

    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(load_file, file) for file in files]
        i = 0
        for future in as_completed(futures):
            future.result()
            i += 1
            logging.info(f"Loaded data from {i} files")

    return data


def transform_data(data, max_threads=16):
    transformed_data = {}

    def transform_row(row):
        max_count = row.get("__max__")
        if max_count is None:
            logging.error(f"Row {row['key']} does not have a __max__ value")
            pass
        for v in row:
            if v != "__max__" and v != "__url__" and v != "key":
                tf = 0.5 + 0.5 * float(float(row[v]) / float(max_count))
                if v in transformed_data:
                    transformed_data[v].append((row["__url__"], tf))
                else:
                    transformed_data[v] = [(row["__url__"], tf)]

    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(transform_row, row) for row in data]
        i = 0
        for future in as_completed(futures):
            i += 1
            future.result()
            logging.info(f"Transformed data from {i} rows")

    return transformed_data


def write_term_file(key, data, output_dir):
    """
    Writes a single term's data to a file.

    Args:
        key (str): The term (file name).
        data (list): List of (url, tf) pairs.
        output_dir (str): Directory to write the file to.
    """
    file_path = os.path.join(output_dir, f"{key}")
    with open(file_path, "w") as f:
        f.write(f"{key} ")  # Write the term as the first line
        for url, tf in data:
            f.write(f"{url},{tf} ")  # Write all (url, tf) pairs on the same line


def write_transformed_data(data, output_dir, max_threads=16):
    """
    Writes transformed data to files using multithreading.

    Args:
        data (dict): A dictionary where keys are terms and values are lists of (url, tf) pairs.
        output_dir (str): Directory to save the transformed files.
        max_threads (int): Maximum number of threads to use for writing files.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Use ThreadPoolExecutor for multithreaded writing
    with ThreadPoolExecutor(max_threads) as executor:
        futures = [
            executor.submit(write_term_file, key, data[key], output_dir) for key in data
        ]

        # Ensure all threads complete
        i = 0
        for future in futures:
            i += 1
            logging.info(f"Writing {i} transformed data files")
            future.result()


def upload_files_to_s3(bucket_name, data_dir, max_threads=16):
    """
    Uploads files from a directory to an S3 bucket. Creates the bucket if it doesn't exist.

    Args:
        bucket_name (str): Name of the S3 bucket.
        data_dir (str): Directory containing the files to upload.
        max_threads (int): Maximum number of threads to use for uploading.
    """

    def ensure_bucket_exists(bucket_name):
        """
        Ensures the bucket exists. Creates the bucket if it doesn't exist.
        Handles region-specific constraints.
        """
        try:
            # Check if the bucket exists
            s3.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket {bucket_name} already exists.")
        except botocore.exceptions.ClientError as e:
            # If bucket does not exist, attempt to create it
            if e.response["Error"]["Code"] == "404":
                logging.info(f"Bucket {bucket_name} does not exist. Creating it now.")
                try:
                    # Get the region name from the current session
                    region_name = boto3.session.Session().region_name
                    create_bucket_params = {"Bucket": bucket_name}

                    # Only add LocationConstraint if region is not 'us-east-1'
                    if region_name and region_name != "us-east-1":
                        create_bucket_params["CreateBucketConfiguration"] = {
                            "LocationConstraint": region_name
                        }

                    # Create the bucket
                    s3.create_bucket(**create_bucket_params)
                    logging.info(f"Bucket {bucket_name} created successfully.")
                except Exception as bucket_error:
                    logging.error(
                        f"Failed to create bucket {bucket_name}: {bucket_error}"
                    )
                    raise
            else:
                logging.error(f"Unexpected error when checking bucket: {e}")
                raise

    def upload_file(file_path):
        file_name = os.path.basename(file_path)
        s3_key = file_name
        try:
            s3.upload_file(file_path, bucket_name, s3_key)
            logging.info(f"Uploaded {file_path} to {bucket_name}/{s3_key}")
        except Exception as e:
            logging.error(f"Failed to upload {file_path}: {e}")

    # Ensure the bucket exists
    ensure_bucket_exists(bucket_name)

    # Get the list of files to upload
    files = [
        os.path.join(data_dir, file)
        for file in os.listdir(data_dir)
        if os.path.isfile(os.path.join(data_dir, file))
    ]

    # Upload files using multithreading
    with ThreadPoolExecutor(max_threads) as executor:
        futures = [executor.submit(upload_file, file) for file in files]
        i = 0
        for future in as_completed(futures):
            i += 1
            logging.info(f"Uploaded {i} files to S3 Bucket {bucket_name}")
            future.result()


# Main method
if __name__ == "__main__":
    bucket_name = "pt-counts"
    upload_bucket_name = "pt-counts-fast"
    data_dir = "./data/original"
    output_dir = "./data/transformed"

    # Step 1: Download files
    download_files_from_s3_bucket(bucket_name)

    # Step 2: Load data from files
    data = load_data_from_files(data_dir)

    # Step 3: Transform data
    transformed_data = transform_data(data)

    # Step 4: Write transformed data
    write_transformed_data(transformed_data, output_dir)

    # Step 5: Upload transformed data
    upload_files_to_s3(upload_bucket_name, output_dir)
