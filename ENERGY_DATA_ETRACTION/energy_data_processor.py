# energy_data_processor_secure.py
import pandas as pd
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from environment variables
class Config:
    MONGO_URI = os.getenv('MONGO_URI')
    DATABASE_NAME = "africa_energy_db"
    COLLECTION_NAME = "energy_metrics"
    CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')

# Constants
AFRICAN_COUNTRIES = [
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
    "Cameroon", "Cape Verde", "Central African Republic", "Chad", "Comoros",
    "Congo Democratic Republic", "Congo Republic", "Cote d'Ivoire",
    "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia",
    "Gabon", "Gambia", "Ghana", "Guinea", "Guinea Bissau", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
    "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda",
    "Sao Tome and Principe", "Senegal", "Seychelles", "Sierra Leone", "Somalia",
    "South Africa", "South Sudan", "Sudan", "Tanzania", "Togo", "Tunisia",
    "Uganda", "Zambia", "Zimbabwe"
]

INDICATOR_MAPPING = {
    "Population access to electricity-National (% of population)": {
        "metric": "Electricity Access Rate",
        "unit": "%",
        "sector": "Power",
        "sub_sector": "Access",
        "sub_sub_sector": "National"
    }
}

class MongoDBHandler:
    def __init__(self, connection_uri, db_name, collection_name):
        self.client = MongoClient(connection_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
    
    def create_indexes(self):
        indexes = [
            [("country", ASCENDING), ("metric", ASCENDING)],
            [("country_serial", ASCENDING)],
            [("sector", ASCENDING), ("sub_sector", ASCENDING)]
        ]
        for index_fields in indexes:
            self.collection.create_index(index_fields)
    
    def insert_documents(self, documents):
        result = self.collection.insert_many(documents)
        return result.inserted_ids
    
    def close_connection(self):
        self.client.close()

class CSVDataProcessor:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.data = None
    
    def load_csv_data(self):
        self.data = pd.read_csv(self.csv_file_path)
        return self.data
    
    def _get_country_serial(self, country_name):
        country_mapping = {country: idx + 1 for idx, country in enumerate(AFRICAN_COUNTRIES)}
        return country_mapping.get(country_name, 0)
    
    def transform_to_mongodb_format(self):
        if self.data is None:
            self.load_csv_data()
        
        mongodb_documents = []
        
        for index, row in self.data.iterrows():
            country = row['Country']
            indicator = row['Indicator']
            
            indicator_config = INDICATOR_MAPPING.get(indicator, {
                "metric": indicator,
                "unit": "Unknown",
                "sector": "Energy",
                "sub_sector": "General",
                "sub_sub_sector": ""
            })
            
            document = {
                "country": country,
                "country_serial": self._get_country_serial(country),
                "metric": indicator_config["metric"],
                "unit": indicator_config["unit"],
                "sector": indicator_config["sector"],
                "sub_sector": indicator_config["sub_sector"],
                "sub_sub_sector": indicator_config["sub_sub_sector"],
                "source_link": "CSV File: Database_Electricity.csv",
                "source": "Africa Energy Portal Dataset"
            }
            
            year_columns = [col for col in self.data.columns if col.isdigit() and 2000 <= int(col) <= 2024]
            for year_col in year_columns:
                value = row[year_col]
                if pd.notna(value) and value != '':
                    document[year_col] = float(value)
                else:
                    document[year_col] = None
            
            mongodb_documents.append(document)
        
        return mongodb_documents

class DataValidator:
    def validate_data_completeness(self, data):
        validation_report = {
            "total_countries": len(AFRICAN_COUNTRIES),
            "processed_countries": set(),
            "missing_countries": [],
            "metrics_coverage": {},
            "year_coverage": {},
            "completeness_score": 0
        }
        
        processed_countries = set(record['country'] for record in data)
        validation_report["processed_countries"] = processed_countries
        validation_report["missing_countries"] = [country for country in AFRICAN_COUNTRIES if country not in processed_countries]
        
        all_metrics = set(record['metric'] for record in data)
        for metric in all_metrics:
            count = sum(1 for record in data if record['metric'] == metric)
            validation_report["metrics_coverage"][metric] = {
                "total": len(AFRICAN_COUNTRIES),
                "available": count,
                "coverage_percentage": round((count / len(AFRICAN_COUNTRIES)) * 100, 2)
            }
        
        required_years = [str(year) for year in range(2000, 2025)]
        total_possible_data_points = len(data) * len(required_years)
        available_data_points = 0
        
        for year in required_years:
            year_data_count = sum(1 for record in data if year in record and record[year] is not None)
            validation_report["year_coverage"][year] = {
                "total_records": len(data),
                "available_data": year_data_count,
                "coverage_percentage": round((year_data_count / len(data)) * 100, 2)
            }
            available_data_points += year_data_count
        
        if total_possible_data_points > 0:
            validation_report["completeness_score"] = round((available_data_points / total_possible_data_points) * 100, 2)
        
        return validation_report

def validate_environment_variables():
    """Validate that all required environment variables are present"""
    required_vars = ['MONGO_URI', 'CSV_FILE_PATH']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        print("Please create a .env file with the following variables:")
        print("MONGO_URI=your_mongodb_connection_string")
        print("CSV_FILE_PATH=your_csv_file_path")
        return False
    return True

def main():
    print("Starting Africa Energy Data Processing from CSV")
    
    # Validate environment variables first
    if not validate_environment_variables():
        return
    
    config = Config()
    db_handler = MongoDBHandler(config.MONGO_URI, config.DATABASE_NAME, config.COLLECTION_NAME)
    csv_processor = CSVDataProcessor(config.CSV_FILE_PATH)
    validator = DataValidator()
    
    print("Creating database indexes")
    db_handler.create_indexes()
    
    print("Loading CSV data")
    csv_data = csv_processor.load_csv_data()
    
    print("Transforming data for MongoDB storage")
    mongodb_documents = csv_processor.transform_to_mongodb_format()
    
    print("Validating transformed data")
    data_validation = validator.validate_data_completeness(mongodb_documents)
    
    print("Storing data in MongoDB")
    inserted_ids = db_handler.insert_documents(mongodb_documents)
    
    df_export = pd.DataFrame(mongodb_documents)
    csv_filename = f"transformed_energy_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_export.to_csv(csv_filename, index=False)
    
    print("DATA PROCESSING COMPLETED SUCCESSFULLY")
    print(f"Total documents processed: {len(mongodb_documents)}")
    print(f"Countries covered: {len(data_validation['processed_countries'])}")
    print(f"Completeness Score: {data_validation['completeness_score']}%")
    print(f"Transformed data exported to: {csv_filename}")
    
    db_handler.close_connection()

def query_sample_data():
    """Function to query and display sample data from MongoDB"""
    if not validate_environment_variables():
        return
    
    config = Config()
    db_handler = MongoDBHandler(config.MONGO_URI, config.DATABASE_NAME, config.COLLECTION_NAME)
    
    sample_data = list(db_handler.collection.find().limit(5))
    
    print("\nSAMPLE DATA FROM MONGODB:")
    print("=" * 50)
    for doc in sample_data:
        print(f"Country: {doc['country']} (Serial: {doc['country_serial']})")
        print(f"Metric: {doc['metric']}")
        print(f"Unit: {doc['unit']}")
        print(f"Sector: {doc['sector']} -> {doc['sub_sector']} -> {doc['sub_sub_sector']}")
        print(f"Sample values: 2000: {doc.get('2000', 'N/A')}, 2022: {doc.get('2022', 'N/A')}")
        print("-" * 30)
        
    db_handler.close_connection()

def check_data_quality():
    """Function to check data quality after processing"""
    if not validate_environment_variables():
        return
    
    config = Config()
    db_handler = MongoDBHandler(config.MONGO_URI, config.DATABASE_NAME, config.COLLECTION_NAME)
    validator = DataValidator()
    
    all_data = list(db_handler.collection.find())
    
    if all_data:
        validation_results = validator.validate_data_completeness(all_data)
        
        print("\nDATA QUALITY CHECK:")
        print("=" * 40)
        print(f"Total Documents: {len(all_data)}")
        print(f"Completeness Score: {validation_results['completeness_score']}%")
        print(f"Countries Covered: {len(validation_results['processed_countries'])}/{validation_results['total_countries']}")
        print(f"Missing Countries: {len(validation_results['missing_countries'])}")
    else:
        print("No data found in MongoDB collection")
        
    db_handler.close_connection()

if __name__ == "__main__":
    main()
    
    print("\n" + "="*50)
    print("QUERYING SAMPLE DATA FROM MONGODB")
    print("="*50)
    query_sample_data()
    
    print("\n" + "="*50)
    print("DATA QUALITY CHECK")
    print("="*50)
    check_data_quality()