import os
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPDigestAuth
import concurrent.futures
import argparse
import mysql.connector
import logging
import signal

# Global flag for Ctrl+C
terminate_flag = False

def cam_logs(profitCameraIP, profitCameraPort, username, password, savefolder):
    # Check for the terminate flag before each download
    if terminate_flag:
        return

    # Create a folder based on the current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = os.path.join(savefolder, current_date)

    # Check if the folder already exists, if not, create it
    os.makedirs(folder_path, exist_ok=True)

    # Define the API endpoint URL
    url = f"http://{profitCameraIP}:{profitCameraPort}/axis-cgi/admin/systemlog.cgi"

    # Create a subfolder based on the IP and port inside the current date folder
    subfolder = os.path.join(folder_path, f"{profitCameraIP}_{profitCameraPort}")

    # Check if the subfolder already exists, if not, create it
    os.makedirs(subfolder, exist_ok=True)

    # Create a filename based on the current time
    current_time = datetime.now().strftime("%H-%M-%S")
    filename = os.path.join(subfolder, f"systemlog_{current_time}.txt")

    # Calculate the retention date (30 days from the current date)
    retention_date = datetime.now() - timedelta(days=30)

    # Make the HTTP request with digest authentication
    try:
        response = requests.get(url, auth=HTTPDigestAuth(username, password))

        if response.status_code == 200:
            # Check if the file's creation time is older than the retention date
            if os.path.exists(filename):
                file_creation_time = datetime.fromtimestamp(os.path.getctime(filename))
                if file_creation_time < retention_date:
                    os.remove(filename)  # Remove the old file

            with open(filename, "wb") as file:
                file.write(response.content)
            logging.info(f"Data saved to {filename}")
        else:
            logging.error(f"Request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")

def sigint_handler(signal, frame):
    global terminate_flag
    print("Ctrl+C detected. Terminating...")
    terminate_flag = True

def get_data_from_database(database_host, database_user, database_password, database_name, table_name):
    try:
        if not database_host:
            logging.error("Database host is not provided.")
            return []

        if not database_user or not database_password:
            logging.error("Database username or password is missing.")
            return []

        if not database_name:
            logging.error("Database name is not provided.")
            return []

        conn = mysql.connector.connect(
            host=database_host,
            user=database_user,
            password=database_password,
            database=database_name
        )

        if not conn.is_connected():
            logging.error("Failed to connect to the database.")
            return []

        cursor = conn.cursor()

        # Example SQL query to select data from a table
        cursor.execute(f'SELECT profitCameraIp, profitCameraPort, username, password FROM {table_name} where cameraType LIKE "A%" AND (analyticId=7 or analyticId like "%H%")')

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
    parser.add_argument("database_host", type=str, help="MySQL database host")
    parser.add_argument("database_user", type=str, help="MySQL database username")
    parser.add_argument("database_password", type=str, help="MySQL database password")
    parser.add_argument("database_name", type=str, help="MySQL database name")
    parser.add_argument("table_name", type=str, help="Name of the database table to retrieve data from")
    parser.add_argument("savefolder", type=str, help="Folder to save the retrieved data")

    args = parser.parse_args()

    if not args.savefolder:
        logging.error("Save folder path is not provided.")
        exit(1)

    return args

def main():
    signal.signal(signal.SIGINT, sigint_handler)

    args = parse_arguments()

    # Create a directory for logs if it doesn't exist
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(args.savefolder, current_date, "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Set up logging to a file
    log_filename = os.path.join(log_dir, "program_log.log")
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

    rows = get_data_from_database(args.database_host, args.database_user, args.database_password, args.database_name, args.table_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(cam_logs, *row, args.savefolder): row for row in rows}
        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"Error for row {row}: {exc}")

if __name__ == "__main__":
    main()
