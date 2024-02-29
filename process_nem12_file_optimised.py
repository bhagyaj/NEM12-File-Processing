import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
import time
import logging
import os.path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    """
    Database class manages database connections and provides methods for interacting with the database.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.connect()
        return cls._instance

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname="flo_test",
                user="postgres",
                password="root",
                host="localhost",
                port="5432"
            )
        except psycopg2.Error as e:
            logger.error("Unable to connect to the database")
            logger.error(e)
            raise e

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def insert_meter_readings(self, meter_readings):
        """
        Insert meter readings into the database.

        :param meter_readings: List of tuples containing meter readings to insert
        """
        try:
            cur = self.conn.cursor()
            cur.executemany(
                "INSERT INTO meter_readings (id, nmi, timestamp, consumption) VALUES (%s, %s, %s, %s)",
                meter_readings
            )
            self.conn.commit()
            cur.close()
            logger.info("Inserted %d meter readings into the database", len(meter_readings))
        except psycopg2.Error as e:
            logger.error("Error inserting meter readings:")
            logger.error(e)
            raise e


def process_200_record(line_data, interval_offset):
    """
    Process a 200 record and extract relevant information.

    :param line_data: List containing the fields of the 200 record
    :param interval_offset: Offset for generating unique timestamps
    :return: Tuple containing NMI, interval length, and updated interval offset
    """
    nmi = line_data[1]
    interval_length = int(line_data[8])
    interval_offset += 1
    return nmi, interval_length, interval_offset


def process_300_record(line_data, current_nmi, interval_length, interval_offset, inserts):
    """
    Process a 300 record and extract relevant information.

    :param line_data: List containing the fields of the 300 record
    :param current_nmi: Current NMI
    :param interval_length: Interval between meter recording
    :param interval_offset: Offset for generating unique timestamps
    :param inserts: List of tuples to record batch data
    """
    current_interval_date = datetime.strptime(line_data[1], '%Y%m%d')
    current_interval_date += timedelta(seconds=10 * interval_offset)

    # Assumes consumption values are recorded until 'A' is encountered.
    interval_values = list(map(float, line_data[2:line_data.index('A')]))
    for record_number, consumption_value in enumerate(interval_values):
        timestamp = current_interval_date + timedelta(minutes=record_number * interval_length)
        inserts.append((str(uuid4()), current_nmi, timestamp, float(consumption_value)))


def process_file(file_path):
    """
    Process the input file and insert meter readings into the database.

    :param file_path: Path to the input file
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file '{file_path}' not found")  # Raise FileNotFoundError if file not found

    db = Database()
    inserts = []
    try:
        with open(file_path, 'r') as file:
            current_nmi = None
            interval_length = None
            interval_offset = 0  # Offset for generating unique timestamps

            for line in file:
                line_data = line.strip().split(',')
                if line_data[0] == '200':
                    current_nmi, interval_length, interval_offset = process_200_record(line_data, interval_offset)
                elif line_data[0] == '300':
                    process_300_record(line_data, current_nmi, interval_length, interval_offset, inserts)

                # Execute database operation only if there are inserts to be made
            if inserts:
                db.insert_meter_readings(inserts)
                logger.info(f"Inserted {len(inserts)} meter readings into the database")
            else:
                logger.info("No meter readings to insert into the database")

    except Exception as e:
        logger.error("Error processing file:", e)
    finally:
        db.close()  # Ensure database connection is closed even in case of exceptions


if __name__ == "__main__":
    start_time = time.time()
    process_file("example_file.txt")
    logger.info("Execution time: %.2f seconds", time.time() - start_time)
