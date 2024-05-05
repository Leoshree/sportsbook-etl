**Sports Betting Data ETL Pipeline**

**Overview**

This project contains an ETL (Extract, Transform, Load) pipeline designed to process sports betting data, specifically for combining betting odds with market information and appending transactional data. This pipeline leverages Python with Pandas for data manipulation and is structured with best practices in software development, including modularity, error handling, and configuration management.

**Project Structure**

sportsbook-etl/

│

├── src/

│   ├── etl.py                - Main ETL script containing the DataETL class and processing logic.

│

├── tests/

│   ├── unit_test_etl.py            - Unit tests for individual components of the ETL pipeline.

│   ├── integration_test_etl.py     - Integration tests that run the entire ETL process.

│

├── config/

│   ├── config.ini            - Configuration file to manage file paths used in the ETL process.

│

├── requirements.txt          - List of dependencies to install.

├── README.md                 - Documentation for setting up and using the ETL pipeline.


**Setup Instructions**

**Prerequisites**

Python 3.8+
Pandas library

**Installation**

**1. Clone the repository:**

git clone https://github.com/Leoshree/sportsbook-etl.git

cd sportsbook-etl


**2. Install required Python packages:**

pip install -r requirements.txt


**Configuration**

Edit the 'config.ini' file in the 'config/' directory to set the file paths for the input data and output files accordingly:

[Paths]

BetsPath = data/bets_v1.parquet

TransactionsPath = data/trans_v1.parquet

OutputPath = output/bets_interview_completed.parquet


**Data Files**

Ensure that the input data files ('bets_v1.parquet' and 'trans_v1.parquet') are placed in the 'data/' directory as specified in the 'config.ini'. The output files will be generated in the 'output/' directory.

**Usage**

To run the ETL pipeline, execute the 'etl.py' script from the command line:

python src/etl.py

This script initializes the ETL process using the configurations specified and processes the input files to produce the consolidated output.

**Classes and Functions**

**DataETL Class:**

Located in 'src/etl.py', this class encapsulates all the ETL logic:

**__init__(self, config_path):** Constructor that initializes the ETL process with path configurations.

**read_parquet(self, file_path):** Reads a Parquet file and returns a DataFrame.

**combine_legs_markets(self, row):** Combines data from the 'legs' and 'markets' columns into a single 'outcomes' list.

**add_transaction(self):** Enhances the DataFrame with 'transactions' details.

**save_data(self, df, path):** Saves the DataFrame to a specified Parquet file.

**Testing**

This project includes both unit and integration tests located in the 'tests/' directory.

**Running Unit Tests**

Run the unit tests to verify individual components:

python -m unittest tests/test_etl.py

**Running Integration Tests**

Run the integration tests to validate the entire ETL process:

python -m unittest tests/test_integration_etl.py

**Contact**

For any inquiries, feedback, or issues, please contact Leoshree at leoshree.mohapatra4learning@gmail.com.
