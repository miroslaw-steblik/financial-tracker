
from utils.mapping import column_mapping
from financial_tracker.models.pipeline import BankDataProcessor
from utils.config import Config

config = Config()

def main():
    processor = BankDataProcessor()
    processor.run(config.bank_folders)

if __name__ == '__main__':
    main()