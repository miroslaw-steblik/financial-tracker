import os
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER', 'default_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'default_password')
POSTGRES_DBNAME = os.getenv('POSTGRES_DBNAME', 'default_dbname')
HOST = os.getenv('HOST', 'localhost')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

class Config:
    def __init__(self):
        self.postgres_user = POSTGRES_USER
        self.postgres_password = POSTGRES_PASSWORD
        self.postgres_dbname = POSTGRES_DBNAME
        self.host = HOST
        self.postgres_schema = POSTGRES_SCHEMA
        self._bank_folders = {
            'barclays': './data/barclays',
            'mbna': './data/mbna'
        }
        self._combined_file_path = "./data/financial_tracker.csv"
    
    @property
    def bank_folders(self):
        return self._bank_folders
    
    @property
    def combined_file_path(self):
        return self._combined_file_path





