import requests
import mysql.connector
import hashlib
import time
import json
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, filename='hotel_data.log',
                    format='%(asctime)s:%(levelname)s:%(message)s')


def generate_api_signature(api_key, secret):
    timestamp = str(int(time.time()))  # Get current timestamp in seconds
    concatenated_string = api_key + secret + timestamp
    signature = hashlib.sha256(concatenated_string.encode()).hexdigest()
    return signature


API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
signature = generate_api_signature(API_KEY, API_SECRET)


def save_json_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)


# Function to fetch data from API
def fetch_facility_data(from_index, to_index):
    url = 'https://api.hotelbeds.com/hotel-content-api/1.0/types/facilities'
    params = {
        'fields': 'all',
        'language': 'ENG',
        'from': from_index,
        'to': to_index,
        'useSecondaryLanguage': True
    }
    headers = {
        'Api-Key': API_KEY,
        'X-Signature': signature,
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from API: {e}")
        return None


# Function to establish MySQL connection
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME')
        )
        return conn
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to MySQL: {err}")
        return None


# Function to insert data into MySQL tables
def insert_data_into_mysql(facility_data, conn):
    if not facility_data or not conn:
        logging.error("Invalid input data or MySQL connection.")
        return

    cursor = conn.cursor()

    try:
        for facility in facility_data.get('facilities', []):
            facility_query = """
                INSERT INTO hb_facilities_data (code, facilityGroupCode, facilityTypologyCode, description)
                VALUES (%s, %s, %s, %s)
            """
            facility_data_to_insert = (
                facility.get('code'),
                facility.get('facilityGroupCode'),
                facility.get('facilityTypologyCode'),
                facility['description'].get('content') if 'description' in facility else None
            )

            # logging.debug(f"Inserting data: {facility_data_to_insert}")
            cursor.execute(facility_query, facility_data_to_insert)

        conn.commit()
        logging.info("Data inserted into MySQL tables successfully.")

    except Exception as e:
        logging.error(f"Error inserting data into MySQL: {e}")
        conn.rollback()

    finally:
        cursor.close()


def main():
    batch_size = 100  # Number of records per batch
    start_index = 1  # Starting index
    end_index = 50000  # End index

    conn = connect_to_mysql()
    if not conn:
        return

    for from_index in range(start_index, end_index + 1, batch_size):
        to_index = min(from_index + batch_size - 1, end_index)
        logging.info(f"Fetching records from {from_index} to {to_index}...")

        facility_data = fetch_facility_data(from_index, to_index)
        if facility_data:
            # logging.debug(f"Fetched data: {facility_data}")
            save_json_to_file(facility_data, f'facilities/facility_data_{from_index}_{to_index}.json')
            insert_data_into_mysql(facility_data, conn)

    conn.close()


if __name__ == "__main__":
    main()
