# query_country.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def query_country_data(country_name):
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client.africa_energy_db
    collection = db.energy_metrics
    
    print(f"\n📊 ENERGY DATA FOR {country_name.upper()}:")
    print("=" * 50)
    
    # Get all metrics for the country
    results = collection.find({"country": country_name})
    
    for doc in results:
        print(f"\n🔹 {doc['metric']} ({doc['unit']})")
        print(f"   Sector: {doc['sector']} → {doc['sub_sector']} → {doc['sub_sub_sector']}")
        
        # Show key years
        years_to_show = ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015', '2020', '2022']
        for year in years_to_show:
            if year in doc and doc[year] is not None:
                print(f"   {year}: {doc[year]}{doc['unit']}")
    
    client.close()

if __name__ == "__main__":
    country = input("Enter country name: ").strip()
    query_country_data(country)
