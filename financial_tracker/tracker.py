import pandas as pd
import os
import boto3
import re

from keywords import keyword_groups

barclays_path = "./data/barclays"
mbna_path = "./data/mbna"
combined_df_path = "./data/financial_tracker.csv"

#--------------------- s3 ----------------------------------------------------#
bucket_name = 'finance-a'
key = 'monthly-budget/financial_tracker.csv'

#--------------------- data validation --------------------------------------------#
def validate_columns(df, columns):
    for column, dtype in columns.items():
        assert column in df.columns, f"Expected column '{column}' not found in DataFrame"
        assert df[column].dtype == dtype, f"Expected '{column}' to have dtype '{dtype}'"
        assert df[column].isnull().sum() == 0, f"Found null values in '{column}'"


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

    print(bdf.info())
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

    print(mdf.info())
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

    # Post-transformation validation
    validate_columns(all_df, post_columns)

    print(all_df.info())
    return all_df

def save_all():
    all_df.to_csv(combined_df_path, index=False)
    print()
    print("Data saved to local file: steblik_finance.csv")
    return all_df

#------------------------------------------------------------------------------#

""" config details for S3 are in ~/.aws/credentials """

def load_s3():
    s3 = boto3.client('s3')

    s3.upload_file(combined_df_path, bucket_name, key)
    print(f"File uploaded to s3://{bucket_name}/{key}")
    return all_df


#-------------------- main -----------------------------------------------------#
if __name__== "__main__":
    bdf = barclays()
    mdf = mbna()
    all_df = combine_data()
    save_all()
    load_s3()
 

