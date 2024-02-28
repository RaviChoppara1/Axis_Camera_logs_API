import os
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPDigestAuth
import concurrent.futures
import argparse
import mysql.connector
import logging
import signal
import boto3
from botocore.exceptions import NoCredentialsError
from io import BytesIO

# Global flag for Ctrl+C
terminate_flag = False

def cam_logs(profitCameraIP, profitCameraPort, username, password, s3_bucket):
    # Check for the terminate flag before each download
    if terminate_flag:
        return

    # Create a folder based on the current date
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Define the API endpoint URL
    url = f"http://{profitCameraIP}:{profitCameraPort}/axis-cgi/admin/systemlog.cgi"

    # Create a subfolder based on the IP and port inside the current date folder
    subfolder = f"{profitCameraIP}_{profitCameraPort}"

    # Create a filename based on the current time
    current_time = datetime.now().strftime("%H-%M-%S")
    filename = f"test/{current_date}/{subfolder}/systemlog_{current_time}.txt"

    # Calculate the rls
    # etention date (30 days from the current date)
    retention_date = datetime.now() - timedelta(days=30)



    # Make the HTTP request with digest authentication
    try:
        response = requests.get(url, auth=HTTPDigestAuth(username, password))

        if response.status_code == 200:
            # Check if the file's creation time is older than the retention date
            if s3_object_exists(s3_bucket, filename):
                file_creation_time = s3_object_creation_time(s3_bucket, filename)
                if file_creation_time < retention_date:
                    s3_delete_object(s3_bucket, filename)  # Remove the old file

            s3_upload_object(s3_bucket, filename, response.content)
            logging.info(f"Data saved to s3://{s3_bucket}/{filename}")

        else:
            logging.error(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")


def s3_upload_object(bucket, key, data):
    try:
        s3 = boto3.client('s3')

        # Convert the content to BytesIO object
        fileobj = BytesIO(data)

        # Upload the file-like object
        s3.upload_fileobj(fileobj, bucket, key)

        print(f"Uploaded to S3: s3://{bucket}/{key}")
    except NoCredentialsError:
        logging.error("Credentials not available for S3 upload.")
        print("S3 upload failed: Credentials not available")

def s3_delete_object(bucket, key):
    try:
        s3 = boto3.client('s3')
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted from S3: s3://{bucket}/{key}")
    except NoCredentialsError:
        logging.error("Credentials not available for S3 delete.")
        print("S3 delete failed: Credentials not available")
def s3_object_exists(bucket, key):
    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def s3_object_creation_time(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    return obj.last_modified

def sigint_handler(signal, frame):
    global terminate_flag
    print("Ctrl+C detected. Terminating...")
    terminate_flag = True

def get_data_from_database( database_user, database_password, database_name, table_name):
    try:
        database_endpoint = "microservice-common-db.pro-vigil.com"
        database_port = 3306

        conn = mysql.connector.connect(
            host=database_endpoint,
            port=database_port,
            user=database_user,
            password=database_password,
            database=database_name
        )

        if not conn.is_connected():
            logging.error("Failed to connect to the database.")
            return []
        print("Database connection successful")
        cursor = conn.cursor()

        # Example SQL query to select data from a table
        cursor.execute(f'SELECT profitCameraIp, profitCameraPort, username, password FROM {table_name} where cameraType="Axis" and (analyticId="7" or analyticId="11" or analyticId= "12" or analyticId="11H" or analyticId= "12H")')

        if not cursor:
            logging.error("Failed to execute SQL query.")
            conn.close()
            return []

        rows = cursor.fetchall()

        conn.close()

        return rows
    except mysql.connector.Error as err:
        logging.error(f"Database connection error: {err}")
        return []

def parse_arguments():
    parser = argparse.ArgumentParser(description="Retrieve data from an API and save it to a file with digest authentication")
    parser.add_argument("database_user", type=str, help="MySQL database username")
    parser.add_argument("database_password", type=str, help="MySQL database password")
    parser.add_argument("database_name", type=str, help="MySQL database name")
    parser.add_argument("table_name", type=str, help="Name of the database table to retrieve data from")
    parser.add_argument("s3_bucket", type=str, help="S3 bucket to save the retrieved data")

    args = parser.parse_args()

    if not args.s3_bucket:
        logging.error("S3 bucket path is not provided.")
        exit(1)

    return args


def main():
    signal.signal(signal.SIGINT, sigint_handler)

    args = parse_arguments()

    # Set up logging to a file
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(args.s3_bucket, current_date, "Docker logs")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, "Docker_log.log")
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

    rows = get_data_from_database(args.database_user, args.database_password, args.database_name,
                                  args.table_name)
    # Upload log file to S3
    log_s3_key = os.path.join(current_date, "Docker logs", "Docker_log.log")
    s3_upload_object(args.s3_bucket, log_s3_key, open(log_filename, 'rb').read())

    logging.info(f"Log file uploaded to s3://{args.s3_bucket}/{log_s3_key}")
    

    # Use ThreadPoolExecutor for concurrent execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for row in rows:
            future = executor.submit(cam_logs, *row, args.s3_bucket)
            futures.append(future)

        # Wait for all futures to complete
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()
