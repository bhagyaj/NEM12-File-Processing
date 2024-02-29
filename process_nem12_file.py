import psycopg2
from datetime import datetime, timedelta
from uuid import uuid4
from collections import defaultdict
import time


class Database:
    # Implements the singleton pattern to maintain a single connection to the database.

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
            print("Unable to connect to the database")
            print(e)

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def insert_meter_reading(self, nmi, timestamp, consumption):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO meter_readings (id, nmi, timestamp, consumption) VALUES (%s, %s, %s, %s)",
                (str(uuid4()), nmi, timestamp, consumption)
            )
            self.conn.commit()
            cur.close()
        except psycopg2.Error as e:
            print("Error inserting meter reading:")
            print(e)


def process_file(file_path):
    db = Database()
    inserts = []
    meter_readings = defaultdict(list)
    with open(file_path, 'r') as file:
        current_nmi = None
        interval_length = None
        current_interval_date = None
        current_timestamp = None
        # Generates a unique timestamp for every set of 300 records belonging to a new 200 record.
        counter = 0
        for line in file:
            line_data = line.strip().split(',')

            if line_data[0] == '200':
                current_nmi = line_data[1]
                interval_length = int(line_data[8])
                counter += 1
            elif line_data[0] == '300':

                current_interval_date = datetime.strptime(line_data[1], '%Y%m%d')
                # Initializes timestamp with a different starting point if it belongs to a different 200 record.
                current_interval_date += timedelta(seconds=10 * counter)

                # Assumes consumption values are recorded until 'A' is encountered.
                interval_values = list(map(float, line_data[2:line_data.index('A')]))
                for record_number, consumption_value in enumerate(interval_values):
                    current_timestamp = current_interval_date + timedelta(minutes=record_number * interval_length)
                    meter_readings[(current_nmi, current_timestamp)] = consumption_value

            # Does nothing if the record is neither 200 nor 300.
            elif line_data[0] in ['100', '500']:
                current_nmi = None
                current_interval_date = None

    # Inserts records individually into the database.
    for (nmi, timestamp), consumption in meter_readings.items():
        db.insert_meter_reading(nmi, timestamp, consumption)

    db.close()
    print("Insert statements generated successfully.")


if __name__ == "__main__":
    start_time = time.time()
    process_file("example_file.txt")
    print("Execution time", time.time() - start_time)
