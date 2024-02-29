# NEM12 File Processor

## Overview
This Python script processes NEM12 format files and inserts meter readings into a PostgreSQL database. The script parses the input file, extracts relevant information from each record, and inserts meter readings into the database table.

## Features
- Efficiently parses large NEM12 files.
- Inserts meter readings into the database using bulk insertion for improved performance.
- Generates unique timestamps for each set of 300 records belonging to a new 200 record.
- Handles missing input files gracefully and logs appropriate error messages.
- Uses a singleton Database class to maintain a single database connection.

## Setup
1. Ensure Python 3.x and PostgreSQL are installed on your system.
2. Install the required Python packages using `pip install -r requirements.txt`.
3. Create a PostgreSQL database named `flo_test`.
4. Use the following SQL script to create the `meter_readings` table in your database:

   ```sql
   CREATE TABLE meter_readings (
       id UUID DEFAULT gen_random_uuid() NOT NULL,
       nmi VARCHAR(10) NOT NULL,
       timestamp TIMESTAMP NOT NULL,
       consumption NUMERIC NOT NULL,
       CONSTRAINT meter_readings_pk PRIMARY KEY (id),
       CONSTRAINT meter_readings_unique_consumption UNIQUE (nmi, timestamp)
   );
5. Update the database connection parameters (dbname, user, password, host, port) in the `Database` class in the `process_nem12_file.py` file.

## Usage
To process an NEM12 file, run the script with the file path as the argument:
```
python process_nem12_file.py example_file.txt
```
To process an NEM12 file for larger dataset, run the script with the file path as the argument:
```
python process_nem12_file_optimised.py example_file.txt
```

## Testing
The script includes unit tests to verify the functionality of individual functions. Run the tests using:
```
python test_process_nem12_file_optimised.py
```
Make sure to update the test file paths and database connection parameters as needed.

## Dependencies
- Python 3.x
- PostgreSQL
- psycopg2 library (Python PostgreSQL adapter)

## Notes
- This script assumes a specific format for the input NEM12 files. Make sure the input files adhere to the specified format.
- The script does not handle concurrent access to the database. Additional measures may be needed for a production environment.
- The script is structured using classes and functions, following Python's best practices for modularization and code organization. This ensures readability and maintainability of the codebase.
- The script includes error handling mechanisms to gracefully handle exceptions and unexpected scenarios. This helps prevent crashes and ensures robustness in handling various input conditions and database interactions.
- Logging statements have been strategically placed throughout the code to capture important information, such as errors, warnings, and informational messages. This facilitates troubleshooting and monitoring of the script's execution, especially in production environments.
- Comments have been added throughout the code to explain its functionality, improve readability, and aid in understanding complex sections. Variable and function names are descriptive, making the code easier to follow.
-  Several optimizations have been implemented to improve the script's performance:
  - Bulk Insertion: Meter readings are inserted into the database using bulk insertion (`executemany`), reducing the number of database transactions and improving insertion speed.
  - Efficient Timestamp Generation: Unique timestamps are generated for each set of 300 records belonging to a new 200 record, minimizing the need for repeated timestamp calculations.
  - Minimized Database Transactions: Database transactions are minimized by collecting all meter readings and inserting them in bulk, reducing overhead and improving overall performance.
  - Batch Processing with islice: The islice function from the itertools module is used for batch processing, enabling efficient memory usage and improved processing speed when reading large input files.

- Basic Testing: Unit tests have been included to verify the functionality of individual functions and ensure that the script behaves as expected. These tests cover critical components of the codebase and help catch bugs early in the development process.

- Optimization Gained: The initial execution time of the script `process_nem12_file.py` was measured at 0.423 seconds. With the implemented optimizations, the current execution time has been reduced to 0.16 seconds. This significant reduction in execution time enhances the script's efficiency and responsiveness, allowing it to process large NEM12 files more quickly and effectively.
- While the script optimizes performance and ensures efficient processing of large datasets, it's crucial to note that it currently lacks robust mechanisms for handling data loss. In a production environment, it's recommended to implement strategies for data redundancy, backup, and recovery to mitigate the risk of data loss in case of failures or errors during processing.
- Consider integrating the script into a CI/CD pipeline for automated testing, building, and deployment in a production environment. CI/CD pipelines help streamline development workflows, improve code quality, and ensure reliable deployments of the script and associated changes.
-  While the provided tests cover basic functionality, there is scope for adding more tests to ensure comprehensive test coverage. Examples of additional tests could include edge cases, handling of invalid input, and testing different scenarios to validate the robustness and reliability of the script under various conditions.







