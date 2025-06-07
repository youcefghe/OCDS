
import logging
import pyodbc
import traceback
import os
import re

from table_creation import create_tables
from data_insertion import insert_json_data

logging.basicConfig(
    filename='process.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_connection():

    server = 'DESKTOP-91AK8MU\\SQLEXPRESS'  
    database = 'ConstructionDB'             
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(connection_string)
        msg = " Database connection established."
        print(msg)
        logging.info(msg)
        return conn
    except Exception as e:
        msg = f" Error connecting to the database: {e}"
        print(msg)
        logging.error(msg)
        raise

def extract_date_from_filename(filename):

    match = re.search(r"_(\d{8})_(\d{8})\.json$", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        msg = "\n Creating tables (with history) if they don't exist..."
        print(msg)
        logging.info(msg)
        create_tables(cursor)
        conn.commit()

        msg = " Tables (and history tables) are ready.\n"
        print(msg)
        logging.info(msg)


        json_directory = "data/json/"  

        if os.path.exists(json_directory) and os.path.isdir(json_directory):
            msg = f"\n Processing all JSON files in '{json_directory}'..."
            print(msg)
            logging.info(msg)

            
            files_with_dates = []
            for filename in os.listdir(json_directory):
                if filename.lower().endswith(".json"):
                    file_path = os.path.join(json_directory, filename)
                    
                    start_date, end_date = extract_date_from_filename(filename)
                    files_with_dates.append((start_date, end_date, filename, file_path))

            
            files_with_dates.sort(key=lambda x: (x[0] or '', x[1] or '', x[2]))

            
            for start_date, end_date, filename, file_path in files_with_dates:
                msg = f"  → Inserting data from: {file_path}"
                print(msg)
                logging.info(msg)
                try:
                    insert_json_data(cursor, file_path=file_path)
                    conn.commit()
                    msg_done = f" Done processing {filename}"
                    print(msg_done)
                    logging.info(msg_done)
                except Exception as ex:
                    msg_error = f"❌ Error inserting {filename}: {ex}"
                    print(msg_error)
                    logging.error(msg_error)
                    traceback.print_exc()
                    conn.rollback()
                    

            msg = " All JSON files processed.\n"
            print(msg)
            logging.info("All JSON files processed.")
        else:
            msg = f" No '{json_directory}' folder found. Skipping JSON processing."
            print(msg)
            logging.warning(msg)

    except Exception as e:
        msg = " An error occurred during execution:"
        print(msg)
        logging.exception(msg)
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        msg = "\n Database connection closed."
        print(msg)
        logging.info("Database connection closed.")

if __name__ == '__main__':
    main()
