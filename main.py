import requests
import os
from requests.auth import HTTPDigestAuth
from datetime import datetime
import argparse
import mysql.connector

def get_data_from_database(database_host, database_user, database_password, database_name, table_name):
    conn = mysql.connector.connect(
        host=database_host,
        user=database_user,
        password=database_password,
        database=database_name
    )
    cursor = conn.cursor()

    # Example SQL query to select data from a table
    cursor.execute(f'SELECT profitCameraIp, profitCameraPort, username, password FROM {table_name}')

    rows = cursor.fetchall()

    conn.close()

    return rows

def main(ip, port, username, password, save_folder):
    # Create a folder based on the current date
    current_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = os.path.join(save_folder, current_date)
    os.makedirs(folder_path, exist_ok=True)

    # Define the API endpoint URL
    url = f"http://{ip}:{port}/axis-cgi/admin/systemlog.cgi"

    # Create a subfolder based on the IP and port inside the current date folder
    subfolder = os.path.join(folder_path, f"{ip}_{port}")
    os.makedirs(subfolder, exist_ok=True)

    # Create a filename based on the current time
    current_time = datetime.now().strftime("%H-%M-%S")
    filename = os.path.join(subfolder, f"systemlog_{current_time}.txt")

    # Make the HTTP request with digest authentication
    try:
        response = requests.get(url, auth=HTTPDigestAuth(username, password))

        if response.status_code == 200:
            # Save the response data to a file
            with open(filename, "wb") as file:
                file.write(response.content)
            print(f"Data saved to {filename}")
        else:
            print(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Request error: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retrieve data from an API and save it to a file with digest authentication")
    parser.add_argument("database_host", type=str, help="MySQL database host")
    parser.add_argument("database_user", type=str, help="MySQL database username")
    parser.add_argument("database_password", type=str, help="MySQL database password")
    parser.add_argument("database_name", type=str, help="MySQL database name")
    parser.add_argument("table_name", type=str, help="Name of the database table to retrieve data from")
    parser.add_argument("save_folder", type=str, help="Folder to save the retrieved data")

    args = parser.parse_args()
    rows = get_data_from_database(args.database_host, args.database_user, args.database_password, args.database_name, args.table_name)

    for row in rows:
        main(*row, args.save_folder)
