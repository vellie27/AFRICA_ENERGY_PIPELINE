# energy_data_processor.py
import pandas as pd
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=== AFRICA ENERGY DATA PROCESSOR INNIT,EIIISH ===")
print("Loading environment variables,MTCHEEEEEW...")

# Configuration from environment variables
MONGO_URI = os.getenv('MONGO_URI')
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')
DATABASE_NAME = "africa_energy_db"
COLLECTION_NAME = "energy_metrics"

print(f"✓ MONGO_URI loaded SUCCESSFULLY..BAIDHA MONGO SO MCHEZO: {bool(MONGO_URI)}")
print(f"✓ CSV_FILE_PATH loaded,THOUGH NILI'SWEAT KIASI: {bool(CSV_FILE_PATH)}")

if not MONGO_URI:
    print("❌ ERROR: MONGO_URI not found in .env file!,OBVIOUS,IT'LL WORK")
    exit(1)

if not CSV_FILE_PATH:
    print("❌ ERROR: CSV_FILE_PATH not found in .env file!,OBVIOUS,IT'LL WORK")
    exit(1)

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
    },
    "Energy: Population with access to clean cooking fuels (% of population)": {
        "metric": "Clean Cooking Access Rate",
        "unit": "%",
        "sector": "Energy",
        "sub_sector": "Access",
        "sub_sub_sector": "Clean Cooking"
    },
    "Energy: Population without access to clean cooking fuels (millions of people)": {
        "metric": "Clean Cooking Access Gap",
        "unit": "millions",
        "sector": "Energy",
        "sub_sector": "Access Gap",
        "sub_sub_sector": "Clean Cooking"
    },
    "Energy intensity level of primary energy (MJ/2017 PPP GDP)": {
        "metric": "Energy Intensity",
        "unit": "MJ/2017 PPP GDP",
        "sector": "Energy Efficiency",
        "sub_sector": "Intensity",
        "sub_sub_sector": "Primary Energy"
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
            [("sector", ASCENDING), ("sub_sector", ASCENDING)],
            [("metric", ASCENDING)]
        ]
        for index_fields in indexes:
            self.collection.create_index(index_fields)
    
    def insert_documents(self, documents):
        result = self.collection.insert_many(documents)
        return result.inserted_ids
    
    def get_document_count(self):
        return self.collection.count_documents({})
    
    def get_metrics_summary(self):
        pipeline = [
            {"$group": {
                "_id": "$metric",
                "count": {"$sum": 1},
                "countries": {"$addToSet": "$country"}
            }}
        ]
        return list(self.collection.aggregate(pipeline))
    
    def close_connection(self):
        self.client.close()

class CSVDataProcessor:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.data = None
    
    def load_csv_data(self):
        print(f"Reading CSV from: {self.csv_file_path}")
        try:
            self.data = pd.read_csv(self.csv_file_path)
            print(f"✓ Successfully loaded CSV with {len(self.data)} rows")
            return self.data
        except Exception as e:
            print(f"❌ ERROR reading CSV file: {e}")
            raise
    
    def _get_country_serial(self, country_name):
        country_mapping = {country: idx + 1 for idx, country in enumerate(AFRICAN_COUNTRIES)}
        return country_mapping.get(country_name, 0)
    
    def transform_to_mongodb_format(self):
        if self.data is None:
            self.load_csv_data()
        
        mongodb_documents = []
        processed_metrics = set()
        
        print(f"Processing {len(self.data)} rows from CSV...")
        
        for index, row in self.data.iterrows():
            country = row['Country']
            indicator = row['Indicator']
            
            if indicator not in INDICATOR_MAPPING:
                continue
            
            indicator_config = INDICATOR_MAPPING[indicator]
            processed_metrics.add(indicator)
            
            document = {
                "country": country,
                "country_serial": self._get_country_serial(country),
                "metric": indicator_config["metric"],
                "unit": indicator_config["unit"],
                "sector": indicator_config["sector"],
                "sub_sector": indicator_config["sub_sector"],
                "sub_sub_sector": indicator_config["sub_sub_sector"],
                "source_link": "Africa Energy Portal Dataset",
                "source": "World Bank/International Energy Agency"
            }
            
            year_columns = [col for col in self.data.columns if col.isdigit() and 2000 <= int(col) <= 2024]
            for year_col in year_columns:
                value = row[year_col]
                if pd.notna(value) and value != '' and value != 'NULL':
                    try:
                        if indicator_config["unit"] == "millions":
                            document[year_col] = float(value)
                        else:
                            document[year_col] = float(value)
                    except (ValueError, TypeError):
                        document[year_col] = None
                else:
                    document[year_col] = None
            
            mongodb_documents.append(document)
        
        print(f"✓ Processed metrics: {list(processed_metrics)}")
        return mongodb_documents

class DataValidator:
    def validate_data_completeness(self, data):
        validation_report = {
            "total_countries": len(AFRICAN_COUNTRIES),
            "processed_countries": set(),
            "missing_countries": [],
            "metrics_coverage": {},
            "year_coverage": {},
            "completeness_score": 0,
            "total_documents": len(data)
        }
        
        if not data:
            return validation_report
        
        processed_countries = set(record['country'] for record in data)
        validation_report["processed_countries"] = processed_countries
        validation_report["missing_countries"] = [
            country for country in AFRICAN_COUNTRIES 
            if country not in processed_countries
        ]
        
        all_metrics = set(record['metric'] for record in data)
        for metric in all_metrics:
            count = sum(1 for record in data if record['metric'] == metric)
            validation_report["metrics_coverage"][metric] = {
                "total": len(AFRICAN_COUNTRIES),
                "available": count,
                "coverage_percentage": round((count / len(AFRICAN_COUNTRIES)) * 100, 2)
            }
        
        required_years = [str(year) for year in range(2000, 2023)]
        total_possible_data_points = len(data) * len(required_years)
        available_data_points = 0
        
        for year in required_years:
            year_data_count = sum(
                1 for record in data 
                if year in record and record[year] is not None
            )
            validation_report["year_coverage"][year] = {
                "total_records": len(data),
                "available_data": year_data_count,
                "coverage_percentage": round((year_data_count / len(data)) * 100, 2)
            }
            available_data_points += year_data_count
        
        if total_possible_data_points > 0:
            validation_report["completeness_score"] = round(
                (available_data_points / total_possible_data_points) * 100, 2
            )
        
        return validation_report

def main():
    try:
        print("\n1. Connecting to MongoDB...")
        db_handler = MongoDBHandler(MONGO_URI, DATABASE_NAME, COLLECTION_NAME)
        print("   ✓ Connected to MongoDB successfully")
        
        db_handler.client.admin.command('ping')
        print("   ✓ Database ping successful")
        
        print("\n2. Creating database indexes...")
        db_handler.create_indexes()
        print("   ✓ Indexes created successfully")
        
        print("\n3. Loading and processing CSV data...")
        csv_processor = CSVDataProcessor(CSV_FILE_PATH)
        csv_data = csv_processor.load_csv_data()
        print(f"   ✓ Loaded {len(csv_data)} rows from CSV")
        
        print("\n4. Transforming data for MongoDB...")
        mongodb_documents = csv_processor.transform_to_mongodb_format()
        print(f"   ✓ Transformed {len(mongodb_documents)} documents")
        
        print("\n5. Validating data completeness...")
        validator = DataValidator()
        data_validation = validator.validate_data_completeness(mongodb_documents)
        print("   ✓ Data validation completed")
        
        print("\n6. Storing data in MongoDB...")
        if mongodb_documents:
            db_handler.collection.delete_many({})
            print("   ✓ Cleared existing data")
            
            batch_size = 50
            for i in range(0, len(mongodb_documents), batch_size):
                batch = mongodb_documents[i:i + batch_size]
                db_handler.insert_documents(batch)
            print(f"   ✓ Successfully inserted {len(mongodb_documents)} documents")
        
        print("\n7. Exporting transformed data for verification...")
        df_export = pd.DataFrame(mongodb_documents)
        csv_filename = f"transformed_energy_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_export.to_csv(csv_filename, index=False)
        print(f"   ✓ Data exported to: {csv_filename}")
        
        print("\n8. Generating final report...")
        total_docs = db_handler.get_document_count()
        metrics_summary = db_handler.get_metrics_summary()
        
        print(f"\n📊 FINAL RESULTS:")
        print("=" * 50)
        print(f"Total documents in database: {total_docs}")
        print(f"Countries covered: {len(data_validation['processed_countries'])}/{data_validation['total_countries']}")
        print(f"Data completeness score: {data_validation['completeness_score']}%")
        
        print(f"\n📈 METRICS SUMMARY:")
        for metric_info in metrics_summary:
            print(f"  • {metric_info['_id']}: {metric_info['count']} countries")
        
        print(f"\n🌍 SAMPLE DATA BY METRIC TYPE:")
        sample_queries = [
            {"metric": "Electricity Access Rate"},
            {"metric": "Clean Cooking Access Rate"}, 
            {"metric": "Clean Cooking Access Gap"},
            {"metric": "Energy Intensity"}
        ]
        
        for query in sample_queries:
            sample = db_handler.collection.find_one(query)
            if sample:
                country = sample['country']
                years_data = {k: v for k, v in sample.items() if k.isdigit() and v is not None}
                recent_year = max(years_data.keys()) if years_data else "N/A"
                recent_value = years_data.get(recent_year, "N/A")
                print(f"  • {sample['metric']} - {country}: {recent_value}{sample['unit']} ({recent_year})")
        
        print(f"\n🎉 DATA PROCESSING COMPLETED SUCCESSFULLY!MSICHANA GENIOUS")
        print("=" * 50)
        print(f"✅ All {len(INDICATOR_MAPPING)} energy metrics processed")
        print(f"✅ {total_docs} documents stored in MongoDB")
        print(f"✅ {len(data_validation['processed_countries'])} African countries covered")
        print(f"✅ CSV file successfully processed")
        print(f"✅ Data exported to: {csv_filename}")
        print(f"✅ Database: {DATABASE_NAME}.{COLLECTION_NAME}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("Please check:")
        print("   - MongoDB connection string")
        print("   - CSV file path and format")
        print("   - Internet connection for MongoDB Atlas")
    finally:
        if 'db_handler' in locals():
            db_handler.close_connection()
            print("   ✓ Database connection closed,AND VELLIE IS HAPPY")

if __name__ == "__main__":
    main()