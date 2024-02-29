import unittest
from process_nem12_file_optimised import process_200_record, process_300_record,process_file,Database
from datetime import datetime, timedelta

class TestRecordProcessing(unittest.TestCase):
    # Test for processing 200 records
    def test_process_200_record(self):
        line_data = ['200', 'NEM1201009', 'E1E2', '1', 'E1', 'N1', '01009', 'kWh', '30', '20050610']
        interval_offset = 0
        expected_result = ('NEM1201009', 30, 1)
        self.assertEqual(process_200_record(line_data, interval_offset), expected_result)

    # Test for processing 300 records
    def test_process_300_record(self):
        line_data = ['300', '20050301', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0.461', '0.810',
                     '0.568', '1.234', '1.353', '1.507', '1.344', '1.773', '0', '.848', '1.271', '0.895', '1.327',
                     '1.013', '1.793', '0.988', '0.985', '0.876', '0.555', '0.760', '0.938', '0.566', '0.512', '0.970',
                     '0.760', '0.731', '0.615', '0.886', '0.531', '0.774', '0.712', '0.598', '0.670', '0.587', '0.657',
                     '0.345', '0.231', 'A', '', '', '20050310121004', '20050310182204']
        current_nmi = 'NEM1201009'
        interval_length = 30
        interval_offset = 1
        inserts = []

        # Manually calculate expected timestamp and consumption values based on the provided line data
        expected_inserts = []
        current_timestamp = datetime.strptime(line_data[1], '%Y%m%d')
        interval_values = list(map(float, line_data[2:line_data.index('A')]))
        for record_number, consumption_value in enumerate(interval_values):
            timestamp = current_timestamp + timedelta(minutes=record_number * interval_length)
            inserts.append((current_nmi, current_nmi, timestamp, float(consumption_value)))

        # Process the 300 record
        process_300_record(line_data, current_nmi, interval_length, interval_offset, expected_inserts)

        # Compare the generated inserts with the expected inserts
        self.assertEqual(len(inserts), len(expected_inserts))



class TestProcessFile(unittest.TestCase):

    # Test for file not found scenario
    def test_file_not_found(self):
        file_path = "nonexistent_file.txt"
        with self.assertRaises(FileNotFoundError):
            process_file(file_path)

    # Test for successful processing of the file
    def test_file_processing(self):
        file_path = "example_file.txt"
        # Initialize the database connection
        db = Database()
        db.connect()
        process_file(file_path)
        # Verify if the database contains expected number of records
        expected_records = 384
        db.connect()
        cur = db.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM meter_readings")
        count = cur.fetchone()[0]
        self.assertEqual(count, expected_records, "Database contains incorrect number of records")
        db.close()


if __name__ == '__main__':
    unittest.main()
