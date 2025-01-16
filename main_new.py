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
logging.basicConfig(level=logging.INFO, filename='hotel_data2.log',
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
def fetch_hotel_data(from_index, to_index):
    url = 'https://api.hotelbeds.com/hotel-content-api/1.0/hotels'
    params = {
        'fields': 'all',
        'language': 'ENG',
        'from': from_index,
        'to': to_index,
        'useSecondaryLanguage': False
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
def insert_data_into_mysql(hotel_data, conn):
    if not hotel_data or not conn:
        logging.error("Invalid input data or MySQL connection.")
        return

    cursor = conn.cursor()

    try:
        for hotel in hotel_data['hotels']:
            hotel_code = hotel['code']
            
            # Prepare the general data to store in JSON format
            hotel_details = {
                'name': hotel['name']['content'],
                'category_code': hotel['categoryCode'],
                'accommodation_type_code': hotel['accommodationTypeCode'],
                'email': hotel.get('email', None),
                'website': hotel.get('web', None),
                'last_update': hotel['lastUpdate'],
                'S2C': hotel.get('S2C', None),
                'ranking': hotel['ranking'],
                'coordinates': hotel.get('coordinates', {}),
                'city': hotel.get('city', {}).get('content', None),
                'facilities': hotel.get('facilities', []),
                'rooms': hotel.get('rooms', []),
                'images': hotel.get('images', []),
                'phones': hotel.get('phones', []),
                'board_codes': hotel.get('boardCodes', []),
                'address': hotel.get('address', {}).get('content', None)
            }

            # Insert into `hb_hotel_info` table
            insert_query = """
                INSERT INTO hb_hotel_info 
                (hotel_code, hotel_name, category_code, accommodation_type_code, email, website, last_update, S2C, ranking, coordinates, city, facilities, rooms, images, phones, board_codes, address, hotel_details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_data = (
                hotel_code,
                hotel['name']['content'],
                hotel['categoryCode'],
                hotel['accommodationTypeCode'],
                hotel.get('email', None),
                hotel.get('web', None),
                hotel['lastUpdate'],
                hotel.get('S2C', None),
                hotel['ranking'],
                json.dumps(hotel.get('coordinates', {})),
                hotel.get('city', {}).get('content', None),
                json.dumps(hotel.get('facilities', [])),
                json.dumps(hotel.get('rooms', [])),
                json.dumps(hotel.get('images', [])),
                json.dumps(hotel.get('phones', [])),
                json.dumps(hotel.get('boardCodes', [])),
                hotel.get('address', {}).get('content', None),
                json.dumps(hotel_details)  # Optional general hotel details
            )
            cursor.execute(insert_query, insert_data)

        conn.commit()
        logging.info("Data inserted into MySQL tables successfully.")

    except mysql.connector.Error as err:
        logging.error(f"Error inserting data into MySQL table: {err}")
        conn.rollback()

    finally:
        cursor.close()

def main():
    batch_size = 200  # Number of records per batch
    start_index = 1  # Starting index
    end_index = 50000  # End index

    conn = connect_to_mysql()
    if not conn:
        return

    for from_index in range(start_index, end_index, batch_size):
        to_index = from_index + batch_size - 1
        hotel_data = fetch_hotel_data(from_index, to_index)
        if hotel_data:
            save_json_to_file(hotel_data, f'hotel_data_{from_index}_{to_index}.json')
            insert_data_into_mysql(hotel_data, conn)

    conn.close()

if __name__ == "__main__":
    main()
