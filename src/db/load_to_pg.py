import psycopg2
from psycopg2.extras import execute_batch

from utils.config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DBNAME, HOST



#--------------------- postgres -----------------------------------------------#
conn_params_dic = {
    "host"      : HOST,
    "database"  : POSTGRES_DBNAME,
    "user"      : POSTGRES_USER,
    "password"  : POSTGRES_PASSWORD
}

# ----------------------------- load to postgres ---------------------------------#

def load_data_to_postgres(df):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**conn_params_dic)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS financial_tracker CASCADE;")

    # unqoted names are case-insensitive, will be converted to lowercase
    cur.execute("""
        CREATE TABLE IF NOT EXISTS financial_tracker (
            Date date,
            Description text,
            Amount float,
            Type text,
            Category text,
            Subcategory text
        )
    """)
    

    # Define the INSERT statement, lowercase column names to match the PostgreSQL table
    insert_stmt = """
    INSERT INTO financial_tracker (date, description, amount, type, category, subcategory)
    VALUES (%s, %s, %s, %s, %s, %s)
    """


    # Prepare data for insertion
    data_for_insert = [
        (row['Date'], row['Description'], row['Amount'], row['Type'], row['Category'], row['Subcategory'])
        for index, row in df.iterrows()
    ]

    # Execute the INSERT statement in batches
    execute_batch(cur, insert_stmt, data_for_insert, page_size=100)

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

    print("CSV data has been loaded into PostgreSQL using INSERT statements")
