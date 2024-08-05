import pandas as pd
import os

import re

from utils.keywords import keyword_groups, description_mapping
from db.load_to_pg import load_data_to_postgres

barclays_path = "./data/barclays"
mbna_path = "./data/mbna"
combined_file_path = "./data/financial_tracker.csv"

#--------------------- data validation --------------------------------------------#
def validate_columns(df, columns):
    for column, dtype in columns.items():
        assert column in df.columns, f"Expected column '{column}' not found in DataFrame"
        assert df[column].dtype == dtype, f"Expected '{column}' to have dtype '{dtype}'"
        assert df[column].isnull().sum() == 0, f"Found null values in '{column}'"
        assert df[column].notna().all(), f"Found NaN values in '{column}'"
        assert not df[column].eq('').any(), f"Found empty strings in '{column}'"
        assert not df[column].eq(' ').any(), f"Found whitespace strings in '{column}'"
        assert not df[column].eq('nan').any(), f"Found 'nan' strings in '{column}'"


pre_columns = {
    'Date': 'datetime64[ns]',
    'Description': 'object',
    'Amount': 'float64',
    'Type': 'object',
}
post_columns = {
    'Date': 'datetime64[ns]',
    'Description': 'object',
    'Amount': 'float64',
    'Type': 'object',
    'Category': 'object',
    'Subcategory': 'object',
}

def test_no_duplicates(df):
    duplicates = df.duplicated(keep=False)
    print(df[duplicates])
    assert not duplicates.any(), "Found duplicate transactions"


# --------------------- functions ------------------------------------------------- #
def barclays():
    files = os.listdir(barclays_path)
    dfs = []
    # Iterate over each file
    for file in files:
        if file.endswith('.csv'):
            # Construct the full file path
            file_path = os.path.join(barclays_path, file)
            df = pd.read_csv(file_path)
            dfs.append(df)

    bdf = pd.concat(dfs, ignore_index=True)

    # Check for duplicates
    duplicates = bdf.duplicated(keep=False)
    if duplicates.any():
        print("Barclays Warning: Found duplicate transactions:")
        bdf = bdf.drop_duplicates()
        print('Dropped duplicates')

    test_no_duplicates(bdf)

    # Drop columns
    bdf = bdf.drop(columns=['Number','Account','Subcategory'])
    bdf['Memo'] = bdf['Memo'].apply(lambda x: x.split('\t')[0] if isinstance(x, str) else x)
    bdf['Type'] = bdf['Amount'].apply(lambda x: 'Outflow' if x < 0 else 'Inflow')
    move_amount = bdf.pop('Amount')
    bdf.insert(2,'Amount',move_amount)
    bdf.rename(columns={'Memo': 'Description'},inplace=True)
    bdf['Date'] = pd.to_datetime(bdf['Date'],dayfirst=True)

    # Post-transformation validation
    validate_columns(bdf, pre_columns)

    #print(bdf.info())
    return bdf
    


def mbna():
    files = os.listdir(mbna_path)
    dfs = []
    # Iterate over each file
    for file in files:
        if file.endswith('.csv'):
            # Construct the full file path
            file_path = os.path.join(mbna_path, file)
            df = pd.read_csv(file_path)
            dfs.append(df)

    mdf = pd.concat(dfs, ignore_index=True)

    # Check for duplicates
    duplicates = mdf.duplicated(keep=False)
    if duplicates.any():
        print("MBNA Warning: Found duplicate transactions:")
        mdf = mdf.drop_duplicates()
        print('Dropped duplicates')

    test_no_duplicates(mdf)

    # Drop unnecessary columns
    mdf = mdf.drop(columns=['Date entered','Reference','Unnamed: 5'])
    mdf['Amount'] = mdf['Amount'] * -1
    mdf['Type'] = mdf['Amount'].apply(lambda x: 'Outflow' if x < 0 else 'Inflow')
    mdf['Date'] = pd.to_datetime(mdf['Date'],dayfirst=True)


    # Post-transformation validation
    validate_columns(mdf, pre_columns)

    #print(mdf.info())
    return mdf


def categorize_transaction(description):
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


def combine_data():
    all_df = pd.concat([bdf,mdf])
    all_df = all_df.sort_values(by='Date',ascending=False)
    all_df = all_df.rename_axis('index')
    # Apply the categorization function to create new columns with grouped categories and subcategories
    all_df['Category'], all_df['Subcategory'] = zip(*all_df['Description'].apply(categorize_transaction))

    # remove entries "DIRECT DEBIT PAYMENT TO" from description
    condition = (
    all_df['Category'].eq('Financials') & 
    (all_df['Description'].str.contains('DIRECT DEBIT PAYMENT') | all_df['Description'].str.contains('PAYMENT RECEIVED - THAN'))
    )
    all_df = all_df[~condition]

    all_df['Description'] = all_df['Description'].str.strip()
    # Normalize the 'description' column using the description_mapping dictionary
    all_df['Description'] = all_df['Description'].replace(description_mapping)

    # Post-transformation validation
    validate_columns(all_df, post_columns)

    return all_df

def save_all():
    all_df.to_csv(combined_file_path, index=False)
    print()
    print("Data saved to local file: financial_tracker.csv")
    return all_df







#-------------------- main -----------------------------------------------------#
if __name__== "__main__":
    bdf = barclays()
    mdf = mbna()
    all_df = combine_data()
    all_df = save_all()
    load_data_to_postgres(all_df)
 

