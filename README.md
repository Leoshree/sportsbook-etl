ETL Pipeline for Sports Betting Data
Overview
This repository contains an ETL pipeline designed to process sports betting data. It reads data from Parquet files, combines event odds with market information, appends transaction data, and outputs the results into a new Parquet file. This project uses Pandas for data manipulation and is structured to demonstrate best practices in coding, configuration management, and error handling.

Project Structure
rust
Copy code
sportsbook-etl/
│
├── src/
│   └── etl.py               - Main ETL script for processing data.
│
├── tests/
│   ├── test_etl.py          - Unit tests for the ETL functions.
│   └── test_integration_etl.py - Integration tests for the entire ETL pipeline.
│
├── config/
│   └── config.ini           - Configuration file for ETL paths.
│
└── README.md
Setup Instructions
Prerequisites
Python 3.8 or higher
pip (Python package installer)
Installation
Clone the repository (or download the zip file and extract it):
bash
Copy code
git clone https://your-repository-url.com/sportsbook-etl
cd sportsbook-etl
Install required Python packages:
bash
Copy code
pip install -r requirements.txt
Configuration
Modify the config.ini file in the config/ directory to update the file paths according to your local setup:

ini
Copy code
[Paths]
BetsPath = path/to/bets_v1.parquet
TransactionsPath = path/to/trans_v1.parquet
OutputPath = path/to/bets_interview_completed.parquet
Running the ETL Pipeline
To run the ETL process, execute the following command from the root directory of the project:

bash
Copy code
python src/etl.py
This will process the data as configured in config.ini and output the results to the specified output file.

Testing
This project includes both unit and integration tests.

Running Unit Tests
To run unit tests, navigate to the project root and execute:

bash
Copy code
python -m unittest tests/test_etl.py
Running Integration Tests
To run integration tests, use:

bash
Copy code
python -m unittest tests/test_integration_etl.py
These tests will ensure that all components of the ETL process work correctly together.

Contact
For any additional questions or feedback, please contact [Your Name] at [your.email@example.com].