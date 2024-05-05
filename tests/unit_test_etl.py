import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
from configparser import ConfigParser

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.etl import DataETL

class TestDataETL(unittest.TestCase):

    def setUp(self):
        """Prepare resources for each test, mocking the config values."""
        self.config = ConfigParser()
        self.config['Paths'] = {
            'BetsPath': r'/path/to/test/bets_v1.parquet',
            'TransactionsPath': r'/path/to/test/trans_v1.parquet',
            'OutputPath': r'/path/to/test/bets_interview_completed.parquet'
        }

        with patch('src.etl.ConfigParser') as MockConfigParser:
            instance = MockConfigParser.return_value
            instance.read.return_value = True
            instance.get.side_effect = lambda section, option, fallback=None: self.config[section][option] if self.config.has_option(section, option) else fallback
            self.etl = DataETL('dummy_path')

    @patch('pandas.read_parquet')
    def test_read_parquet(self, mock_read):
        """Test that the Parquet file is read correctly."""
        mock_read.return_value = pd.DataFrame({'data': [1, 2, 3]})
        df = self.etl.read_parquet(self.etl.bets_path)
        mock_read.assert_called_once_with(self.etl.bets_path)
        self.assertIsInstance(df, pd.DataFrame)

    @patch('pandas.read_parquet')
    def test_missing_parquet_file(self, mock_read):
        """Test the behavior when the Parquet file is missing."""
        mock_read.side_effect = FileNotFoundError("The file was not found.")
        with self.assertRaises(SystemExit):
            self.etl.read_parquet(self.etl.bets_path)

    @patch('pandas.read_parquet')
    def test_corrupted_parquet_file(self, mock_read):
        """Test the behavior when the Parquet file is corrupted or empty."""
        mock_read.side_effect = pd.errors.EmptyDataError("No data found in the file.")
        with self.assertRaises(SystemExit):
            self.etl.read_parquet(self.etl.bets_path)

    def test_combine_legs_markets(self):
        """Test the combine_legs_markets method handles data correctly with matching marketRef."""
        # Mock input data
        row = pd.Series({
            'legs': [
                {"price": {"num": 7, "den": 4, "decimal": "2.75", "americanOdds": "+175"},
                 "result": "-",
                 "legPart": {"outcomeRef": "35487833", "marketRef": "10303766", "eventRef": "224919"}},
                {"price": {"num": 89, "den": 100, "decimal": "1.89", "americanOdds": "-113"},
                 "result": "-",
                 "legPart": {"outcomeRef": "35516148", "marketRef": "10309553", "eventRef": "225468"}},
                {"price": {"num": 29, "den": 50, "decimal": "1.58", "americanOdds": "-173"},
                 "result": "W",
                 "legPart": {"outcomeRef": "35506845", "marketRef": "99999999", "eventRef": "225245"}}
            ],
            'markets': [
                {"outcomeRef": "35487833", "marketRef": "10303766", "eventRef": "224919",
                 "categoryRef": "FOOTBALL", "eventName": "Kozakken Boys Werkendam v Amsterdamsche FC",
                 "eventStartTime": "2022-03-22T19:00:00.000+0000"},
                {"outcomeRef": "35516148", "marketRef": "10309553", "eventRef": "225468",
                 "categoryRef": "FOOTBALL", "eventName": "Sandnes Ulf v Egersunds IK",
                 "eventStartTime": "2022-03-22T17:00:00.000+0000"}
            ]
        })

        # Expected output
        expected = [
            {
                **row['legs'][0],
                **row['markets'][0]
            },
            {
                **row['legs'][1],
                **row['markets'][1]
            },
            {
                **row['legs'][2],
                'missing_market': True
            }
        ]

        # Running the combine function
        result = self.etl.combine_legs_markets(row)
        # Asserting the output
        self.assertEqual(result, expected)

    @patch('pandas.DataFrame.to_parquet')
    def test_save_data(self, mock_save):
        """Verify that the DataFrame is saved correctly."""
        df = pd.DataFrame({'data': [1, 2, 3]})
        self.etl.save_data(df, self.etl.output_path)
        mock_save.assert_called_once_with(self.etl.output_path, index=False)

    def test_error_handling_read_parquet(self):
        """Test additional error handling for reading Parquet files."""
        with patch('pandas.read_parquet', side_effect=Exception("Read error")):
            with self.assertRaises(SystemExit):
                self.etl.read_parquet(self.etl.bets_path)

if __name__ == '__main__':
    unittest.main()