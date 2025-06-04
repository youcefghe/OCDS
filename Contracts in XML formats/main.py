import logging
import pyodbc
import traceback
import os
import re

from table_creation import create_tables
from data_insertion import (
    process_avis_file,
    process_contrats_file,
    process_depenses_file
)

logging.basicConfig(
    filename='process.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_connection():
    server = 'DESKTOP-91AK8MU\\SQLEXPRESS'
    database = 'XMLData'
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        print("✅ Database connection established.")
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to the database: {e}")
        logging.error(f"Error connecting: {e}")
        raise

def extract_date_from_filename(filename):
    """
    Extracts date range from filenames like 'Avis_20240501_20240531.xml'
    Returns (start_date, end_date) or (None, None)
    """
    match = re.search(r"_(\d{8})_(\d{8})\.xml", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("🔨 Creating tables if they don't exist...")
        create_tables(cursor)
        conn.commit()
        print("✅ Tables are ready.\n")

        xml_dir = "xml"
        if os.path.exists(xml_dir) and os.path.isdir(xml_dir):
            print(f"📂 Processing all XML files in '{xml_dir}'...")
            logging.info(f"Processing XML in {xml_dir}")

            files_with_dates = []
            
            for filename in os.listdir(xml_dir):
                if not filename.lower().endswith(".xml"):
                    continue
                
                file_path = os.path.join(xml_dir, filename)
                start_date, end_date = extract_date_from_filename(filename)
                is_revision = "revisions" in filename.lower()

                files_with_dates.append((start_date, end_date, is_revision, filename, file_path))

            # Sort so that earlier date ranges come first
            files_with_dates.sort(key=lambda x: (x[0], x[1], x[2]))

            for start_date, end_date, is_revision, filename, file_path in files_with_dates:
                print(f"  → Inserting data from: {file_path}")
                logging.info(f"Inserting from {file_path}")

                try:
                    lower_file = filename.lower()
                    if "avis" in lower_file:
                        process_avis_file(cursor, file_path)
                    elif "contrats" in lower_file:
                        process_contrats_file(cursor, file_path)
                    elif "depenses" in lower_file:
                        process_depenses_file(cursor, file_path)
                    else:
                        print(f"⚠ Unknown file type: {filename}")
                        logging.warning(f"Unknown file type: {filename}")

                    conn.commit()
                    print(f"✅ Done with {filename}\n")

                except Exception as ex:
                    print(f"❌ Error parsing {file_path}: {ex}")
                    logging.error(f"Error: {ex}")
                    traceback.print_exc()
                    conn.rollback()
        else:
            print(f"⚠ No '{xml_dir}' folder found. Skipping.")
            logging.warning(f"No {xml_dir} folder found. Skipping.")

    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        logging.error(f"Error in main: {e}")
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("🔌 Database connection closed.")
        logging.info("Database connection closed.")

if __name__ == '__main__':
    main()