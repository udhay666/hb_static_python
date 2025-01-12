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
import logging
import mysql.connector

def insert_data_into_mysql(hotel_data, conn):
    if not hotel_data or not conn:
        logging.error("Invalid input data or MySQL connection.")
        return

    cursor = conn.cursor()

    try:
        for hotel in hotel_data['hotels']:
            table_name = 'hb_hotel_general_info'
            hotel_general_info_query = """
                INSERT INTO hb_hotel_general_info (hotel_code, hotel_name, category_code, accommodation_type_code, email, website, last_update, S2C, ranking)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            hotel_general_info_data = (
                hotel['code'],
                hotel['name']['content'],
                hotel['categoryCode'],
                hotel['accommodationTypeCode'],
                hotel.get('email', None),
                hotel.get('web', None),
                hotel['lastUpdate'],
                hotel.get('S2C', None),
                hotel['ranking']
            )
            cursor.execute(hotel_general_info_query, hotel_general_info_data)
            hotel_code = hotel['code']

            if 'coordinates' in hotel:
                table_name = 'hb_location_coordinates'
                location_coordinates_query = """
                    INSERT INTO hb_location_coordinates (hotel_code, longitude, latitude, country_code, state_code, destination_code, zone_code, city)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                location_coordinates_data = (
                    hotel_code,
                    hotel['coordinates'].get('longitude'),
                    hotel['coordinates'].get('latitude'),
                    hotel['countryCode'],
                    hotel['stateCode'],
                    hotel['destinationCode'],
                    hotel['zoneCode'],
                    hotel['city']['content'] if 'city' in hotel and 'content' in hotel['city'] else None
                )
                cursor.execute(location_coordinates_query, location_coordinates_data)
            table_name ='hb_description'
            description_query = """
                INSERT INTO hb_description (hotel_code, description_text)
                VALUES (%s, %s)
            """
            description_data = (
                hotel_code,
                hotel['description']['content'] if 'description' in hotel and 'content' in hotel['description'] else ''
            )
            cursor.execute(description_query, description_data)

            for facility in hotel['facilities']:
                table_name = 'hb_facilities'
                facilities_query = """
                    INSERT INTO hb_facilities (hotel_code, facility_code, facility_group_code, number, voucher)
                    VALUES (%s, %s, %s, %s, %s)
                """
                facilities_data = (
                    hotel_code,
                    facility['facilityCode'],
                    facility['facilityGroupCode'],
                    facility.get('number', 0),
                    facility.get('voucher', False)
                )
                cursor.execute(facilities_query, facilities_data)

            if 'rooms' in hotel:
                for room in hotel['rooms']:
                    room_code = room.get('roomCode', None)
                    if not room_code:
                        logging.error(f"Missing room_code for hotel_code: {hotel_code}, room data: {room}")
                        continue
                    table_name = 'hb_rooms_type'
                    rooms_query = """
                        INSERT INTO hb_rooms_type (hotel_code, room_code, room_type, characteristic_code, min_pax, max_pax, min_adults, max_adults, max_children, is_parent_room)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    rooms_data = (
                        hotel_code,
                        room_code,
                        room['roomType'],
                        room['characteristicCode'],
                        room['minPax'],
                        room['maxPax'],
                        room['minAdults'],
                        room['maxAdults'],
                        room['maxChildren'],
                        room['isParentRoom']
                    )
                    cursor.execute(rooms_query, rooms_data)
                    room_id = cursor.lastrowid

                    if 'roomFacilities' in room:
                        for feature in room['roomFacilities']:
                            table_name = 'hb_room_features'
                            room_features_query = """
                                INSERT INTO hb_room_features (room_id, facility_code, facility_group_code, ind_logic, number, voucher)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            room_features_data = (
                                room_id,
                                feature['facilityCode'],
                                feature['facilityGroupCode'],
                                feature.get('indLogic', None),
                                feature.get('number', None),
                                feature.get('voucher', None)
                            )
                            cursor.execute(room_features_query, room_features_data)

                    if 'roomStays' in room:
                        for stay in room['roomStays']:
                            table_name ='hb_room_stays'
                            stay_query = """
                                INSERT INTO hb_room_stays (room_id, stay_type, `orderid`, description)
                                VALUES (%s, %s, %s, %s)
                            """
                            stay_data = (
                                room_id,
                                stay['stayType'],
                                stay['order'],
                                stay.get('description', None)
                            )
                            cursor.execute(stay_query, stay_data)
                            stay_id = cursor.lastrowid

                            if 'roomStayFacilities' in stay:
                                for stay_facility in stay['roomStayFacilities']:
                                    table_name='hb_room_stay_facilities'
                                    room_stay_facilities_query = """
                                        INSERT INTO hb_room_stay_facilities (stay_id, facility_code, facility_group_code, number)
                                        VALUES (%s, %s, %s, %s)
                                    """
                                    room_stay_facilities_data = (
                                        stay_id,
                                        stay_facility['facilityCode'],
                                        stay_facility['facilityGroupCode'],
                                        stay_facility['number']
                                    )
                                    cursor.execute(room_stay_facilities_query, room_stay_facilities_data)

            if 'phones' in hotel:
                for phone in hotel['phones']:
                    table_name='hb_phone_numbers'
                    phone_numbers_query = """
                        INSERT INTO hb_phone_numbers (hotel_code, phone_number, phone_type)
                        VALUES (%s, %s, %s)
                    """
                    phone_numbers_data = (
                        hotel_code,
                        phone['phoneNumber'],
                        phone['phoneType']
                    )
                    cursor.execute(phone_numbers_query, phone_numbers_data)

            if 'boardCodes' in hotel:
                for board_code in hotel['boardCodes']:
                    table_name='hb_board_codes'
                    board_codes_query = """
                        INSERT INTO hb_board_codes (hotel_code, board_code)
                        VALUES (%s, %s)
                    """
                    board_codes_data = (
                        hotel_code,
                        board_code
                    )
                    cursor.execute(board_codes_query, board_codes_data)

            if 'address' in hotel:
                table_name='hb_address'
                address_query = """
                    INSERT INTO hb_address (hotel_code, address, city)
                    VALUES (%s, %s, %s)
                """
                address_data = (
                    hotel_code,
                    hotel['address']['content'],
                    hotel['city']['content'] if 'city' in hotel and 'content' in hotel['city'] else None
                )
                cursor.execute(address_query, address_data)

            if 'images' in hotel:
                for image in hotel['images']:
                    table_name='hb_images'
                    images_query = """
                        INSERT INTO hb_images (hotel_code, image_type_code, path, image_order, visual_order, room_code, room_type, characteristic_code)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    images_data = (
                        hotel_code,
                        image['imageTypeCode'],
                        image['path'],
                        image['order'],
                        image.get('visualOrder', None),
                        image.get('roomCode', None),
                        image.get('roomType', None),
                        image.get('characteristicCode', None)
                    )
                    cursor.execute(images_query, images_data)

        conn.commit()
        logging.info("Data inserted into MySQL tables successfully.")

    except mysql.connector.Error as err:
        # logging.error(f"Error inserting data into MySQL: {err}")
        logging.error(f"Error inserting data into MySQL table: {table_name}, Error: {err}")
        conn.rollback()

    finally:
        cursor.close()



def main():
    batch_size = 100  # Number of records per batch
    start_index = 21401  # Starting index
    end_index = 25000  # End index

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
