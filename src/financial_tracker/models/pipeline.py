import pandas as pd
import os
import re

from utils.keywords import keyword_groups, description_mapping
from utils.mapping import column_mapping 
from db.load_to_pg import load_data_to_postgres
from utils.config import Config

config = Config()

class BankDataProcessor:
    def __init__(self):
        
        self.bank_processors = {
            'barclays': self.barclays_processor,
            'mbna': self.mbna_processor,
            # Add more banks as needed
        }
        self.combined_data = []
        self.column_mapping = column_mapping

    def load_data(self, folder_path, bank_name):
        csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
        column_names = self.column_mapping.get(bank_name, {}).values()
        
        combined_df = pd.concat(
            [pd.read_csv(os.path.join(folder_path, file), usecols=column_names) for file in csv_files],
            ignore_index=True
        )
        return combined_df
    
    def remove_duplicates(self, df):
        duplicates = df.duplicated(keep=False)
        if duplicates.any():
            duplicate_rows = df[duplicates]
            print("Duplicate rows found:")
            print(duplicate_rows)
            df = df.drop_duplicates(keep='first')
            print(f"\nRemoved {duplicates.sum() // 2} duplicate rows from the DataFrame")
        return df
    

    def standardize_data(self, df, bank_name):
        column_mapping = self.column_mapping.get(bank_name, {})  
        reversed_mapping = {v: k for k, v in column_mapping.items()}
        df = df.rename(columns=reversed_mapping)

        if bank_name == "barclays":
            df['description'] = df['description'].apply(lambda x: x.split('\t')[0] if isinstance(x, str) else x)
            df['type'] = df['amount'].apply(lambda x: 'Outflow' if x < 0 else 'Inflow')
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        elif bank_name == "mbna":
            df['amount'] = df['amount'] * -1
            df['type'] = df['amount'].apply(lambda x: 'Outflow' if x < 0 else 'Inflow')
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
            df = df.drop(columns=['reference'])
        return df
    

    def categorize_transaction(self,description):
        # Convert description to lowercase for case-insensitive matching
        description_lower = description.lower()
        
        # Iterate through categories and subcategories
        for category, subcategories in keyword_groups.items():
            for subcategory, keywords in subcategories.items():
                # Check if any keyword matches the description
                for keyword in keywords:
                    # Use regex to match whole words only
                    if re.search(r'\b' + keyword.lower() + r'\b', description_lower):
                        return category, subcategory
        
        # If no keyword matches, return 'None' and 'None' as subcategory
        return 'None', 'None'
    

    def barclays_processor(self, folder_path):
        data = self.load_data(folder_path, "barclays")
        data = self.remove_duplicates(data)
        standardized_data = self.standardize_data(data, "barclays")
        return standardized_data
 
    def mbna_processor(self, folder_path):
        data = self.load_data(folder_path, "mbna")
        data = self.remove_duplicates(data)
        standardized_data = self.standardize_data(data, "mbna")
        return standardized_data
    
    def combine_data(self, data_list):
        # Extend method adds each element of the list to the combined_data list
        self.combined_data.extend(data_list)
        self.combined_data = pd.concat(self.combined_data, ignore_index=True)
        self.combined_data = self.combined_data.sort_values(by='date', ascending=False)
        self.combined_data = self.combined_data.rename_axis('index')
        return self.combined_data

    def normalize_data(self):
        self.combined_data['category'], self.combined_data['subcategory'] = zip(*self.combined_data['description'].apply(self.categorize_transaction))
        
        # Remove entries "DIRECT DEBIT PAYMENT TO" from description
        condition = (
            self.combined_data['category'].eq('Financials') & 
            (self.combined_data['description'].str.contains('DIRECT DEBIT PAYMENT') | self.combined_data['description'].str.contains('PAYMENT RECEIVED - THAN'))
        )
        self.combined_data = self.combined_data[~condition]

        # Use .loc to avoid SettingWithCopyWarning
        self.combined_data.loc[:, 'description'] = self.combined_data['description'].str.strip()
        self.combined_data.loc[:, 'description'] = self.combined_data['description'].replace(description_mapping)
        
        print(f"\nCleaned and normalized data has {self.combined_data.shape[0]} rows and {self.combined_data.shape[1]} columns")
        return self.combined_data
    

    def save_all(self):
        self.combined_data.to_csv(config.combined_file_path, index=False)
        print(f"\nData saved to local file: financial_tracker.csv")
        return self.combined_data
    
    def load_to_database(self, data):
        load_data_to_postgres(data)

    def run(self, bank_folders):
        standardized_data_list = []
        for bank, folder_path in bank_folders.items():
            if bank in self.bank_processors:
                standardized_data = self.bank_processors[bank](folder_path)
                standardized_data_list.append(standardized_data)

        combined_data = self.combine_data(standardized_data_list)
        combined_data = self.normalize_data()
        self.load_to_database(combined_data)
        self.save_all()


    
