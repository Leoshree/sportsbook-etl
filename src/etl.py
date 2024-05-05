import pandas as pd
import logging
from configparser import ConfigParser
import os
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataETL:
    def __init__(self, config_path):
        """
        Initializes the DataETL object with paths to the input and output data files.
        Configurations are loaded from a config file at the specified path.
        """
        self.config = ConfigParser()
        if not self.config.read(config_path):
            logging.error(f"Failed to load config file from {config_path}")
            sys.exit(1)
        
        self.bets_path = self.config.get('Paths', 'BetsPath', fallback=None)
        self.transactions_path = self.config.get('Paths', 'TransactionsPath', fallback=None)
        self.output_path = self.config.get('Paths', 'OutputPath', fallback=None)

        if not all([self.bets_path, self.transactions_path, self.output_path]):
            logging.error("Configuration for file paths is incomplete.")
            sys.exit(1)

    def read_parquet(self, file_path):
        """
        Reads a Parquet file into a pandas DataFrame.
        """
        try:
            return pd.read_parquet(file_path)
        except FileNotFoundError:
            logging.error(f"The file {file_path} was not found.")
            sys.exit(1)
        except pd.errors.EmptyDataError:
            logging.error(f"No data: The file {file_path} is empty or corrupted.")
            sys.exit(1)
        except Exception as e:
            logging.error(f"An error occurred while reading {file_path}: {e}")
            sys.exit(1)

    def combine_legs_markets(self, row):
        """
        Combines data from 'legs' and 'markets' columns into a single 'outcomes' list. It makes it clear what are the odds for a specific market
        """
        legs_dict = {leg['legPart']['marketRef']: leg for leg in row['legs'] if 'legPart' in leg and 'marketRef' in leg['legPart']}
        markets_dict = {market['marketRef']: market for market in row['markets'] if 'marketRef' in market}

        outcomes = []
        for marketRef, leg in legs_dict.items():
            market = markets_dict.get(marketRef, {'missing_market': True})
            outcomes.append({**leg, **market})
        return outcomes

    def add_transaction(self):
        """
        Processes the data by reading, combining, merging, and saving as specified by the paths.
        """
        bets_df = self.read_parquet(self.bets_path)
        transactions_df = self.read_parquet(self.transactions_path)
        bets_df['outcomes'] = bets_df.apply(self.combine_legs_markets, axis=1)
        transactions_grouped = transactions_df.groupby('sportsbook_id').agg(transactions=('trans_uuid', list)).reset_index()
        final_df = bets_df.merge(transactions_grouped, how='left', on='sportsbook_id')
        final_df = final_df[['sportsbook_id', 'account_id', 'outcomes', 'transactions']]
        self.save_data(final_df, self.output_path)

    def save_data(self, df, path):
        """
        Saves the DataFrame to a specified Parquet file.
        """
        try:
            df.to_parquet(path, index=False)
            logging.info(f"Data successfully saved to {path}")
        except Exception as e:
            logging.error(f"Failed to save data: {e}")
            sys.exit(1)

def main():
    config_path = 'sportsbook-etl\\config\\config.ini'
    etl = DataETL(config_path)
    etl.add_transaction()

if __name__ == "__main__":
    main()
