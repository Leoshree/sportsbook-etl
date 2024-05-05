import unittest
import pandas as pd

import sys
import os
import tempfile
from configparser import ConfigParser
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.etl import DataETL

class TestETLIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ Set up environment before any tests are run. """
        # Temporary directory setup
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.test_bets_path = os.path.join(cls.temp_dir.name, 'bets_test.parquet')
        cls.test_transactions_path = os.path.join(cls.temp_dir.name, 'transactions_test.parquet')
        cls.test_output_path = os.path.join(cls.temp_dir.name, 'output_test.parquet')

        # Example data for bets and transactions
        cls.bets_data = pd.DataFrame({
            'sportsbook_id': ['1', '2'],
            'account_id': ['111', '222'],
            'legs': [
                [{"price": {"num": 7, "den": 4, "decimal": "2.75", "americanOdds": "+175"},
                  "result": "-",
                  "legPart": {"outcomeRef": "35487833", "marketRef": "10303766", "eventRef": "224919"}}],
                [{"price": {"num": 5, "den": 2, "decimal": "3.5", "americanOdds": "+250"},
                  "result": "-",
                  "legPart": {"outcomeRef": "35516148", "marketRef": "10309553", "eventRef": "225468"}}]
            ],
            'markets': [
                [{"outcomeRef": "35487833", "marketRef": "10303766", "eventRef": "224919",
                  "categoryRef": "FOOTBALL", "eventName": "Kozakken Boys Werkendam v Amsterdamsche FC",
                  "eventStartTime": "2022-03-22T19:00:00.000+0000"}],
                [{"outcomeRef": "35516148", "marketRef": "10309553", "eventRef": "225468",
                  "categoryRef": "FOOTBALL", "eventName": "Sandnes Ulf v Egersunds IK",
                  "eventStartTime": "2022-03-22T17:00:00.000+0000"}]
            ]
        })
        cls.bets_data.to_parquet(cls.test_bets_path)
        cls.transactions_data = pd.DataFrame({
            'sportsbook_id': ['1', '2', '3'],
            'trans_uuid': ['uuid1', 'uuid2', 'uuid3']
        })
        cls.transactions_data.to_parquet(cls.test_transactions_path)

        # Create a temporary config file
        cls.config_path = os.path.join(cls.temp_dir.name, 'test_config.ini')
        config = ConfigParser()
        config['Paths'] = {
            'BetsPath': cls.test_bets_path,
            'TransactionsPath': cls.test_transactions_path,
            'OutputPath': cls.test_output_path
        }
        with open(cls.config_path, 'w') as f:
            config.write(f)

    def test_add_transaction(self):
        """Integration test to verify the ETL process is functioning correctly."""
        etl = DataETL(self.config_path)
        etl.add_transaction()

        # Load the output data to verify contents
        output_df = pd.read_parquet(self.test_output_path)

        # Test if output DataFrame has correct columns
        self.assertIn('sportsbook_id', output_df.columns)
        self.assertIn('account_id', output_df.columns)
        self.assertIn('outcomes', output_df.columns)
        self.assertIn('transactions', output_df.columns)

        # Verify that the data has been processed correctly
        self.assertEqual(len(output_df), 2)  # Expecting two rows as per input data
        self.assertTrue(all(output_df['transactions'].notna()))  # Expecting all transaction fields to be populated

    @classmethod
    def tearDownClass(cls):
        """ Clean up after tests. """
        cls.temp_dir.cleanup()

if __name__ == '__main__':
    unittest.main()