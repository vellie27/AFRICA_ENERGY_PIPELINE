import os
import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class EnergyQueryTool:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self.connect()
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(os.getenv('MONGO_URI'))
            self.db = self.client.africa_energy_db
            self.collection = self.db.energy_metrics
            print("✅ Connected to MongoDB successfully!")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
        return True
    
    def disconnect(self):
        """Close database connection"""
        if self.client:
            self.client.close()
    
    def get_all_countries(self):
        """Get list of all available countries"""
        return sorted(self.collection.distinct("country"))
    
    def get_all_metrics(self):
        """Get list of all available metrics"""
        return sorted(self.collection.distinct("metric"))
    
    def find_similar_countries(self, input_name):
        """Find countries with similar names"""
        all_countries = self.get_all_countries()
        matches = [c for c in all_countries if input_name.lower() in c.lower()]
        return matches
    
    def query_country(self, country_name, show_trend=True):
        """Query data for a specific country"""
        # Case-insensitive search
        results = list(self.collection.find({
            "country": {"$regex": f"^{country_name}$", "$options": "i"}
        }))
        
        if not results:
            similar = self.find_similar_countries(country_name)
            if similar:
                return None, f"Country not found. Did you mean: {', '.join(similar)}?"
            else:
                return None, "Country not found in database."
        
        # Get the correct case from the first result
        correct_country_name = results[0]['country']
        
        return results, correct_country_name
    
    def display_country_data(self, results, country_name):
        """Display formatted country data"""
        print(f"\n{'='*60}")
        print(f"📊 COMPREHENSIVE ENERGY DATA: {country_name.upper()}")
        print(f"{'='*60}")
        
        for doc in results:
            print(f"\n🔹 {doc['metric']} ({doc['unit']})")
            print(f"   Sector: {doc['sector']} → {doc['sub_sector']} → {doc['sub_sub_sector']}")
            
            # Show key trend years
            trend_years = ['2000', '2005', '2010', '2015', '2020', '2022']
            available_data = []
            
            for year in trend_years:
                if year in doc and doc[year] is not None:
                    available_data.append(f"{year}: {doc[year]}{doc['unit']}")
            
            if available_data:
                print(f"   📈 Trend: {' → '.join(available_data)}")
            
            # Calculate improvement (2000 to 2022)
            if '2000' in doc and doc['2000'] is not None and '2022' in doc and doc['2022'] is not None:
                improvement = doc['2022'] - doc['2000']
                arrow = "↗️" if improvement > 0 else "↘️" if improvement < 0 else "➡️"
                print(f"   {arrow} Change (2000-2022): {improvement:+.1f}{doc['unit']}")
    
    def compare_countries(self, country1, country2, metric_name):
        """Compare two countries for a specific metric"""
        results1 = list(self.collection.find({
            "country": {"$regex": f"^{country1}$", "$options": "i"},
            "metric": metric_name
        }))
        
        results2 = list(self.collection.find({
            "country": {"$regex": f"^{country2}$", "$options": "i"},
            "metric": metric_name
        }))
        
        if not results1 or not results2:
            return "One or both countries not found for this metric."
        
        data1 = results1[0]
        data2 = results2[0]
        
        print(f"\n{'='*50}")
        print(f"🆚 COMPARISON: {country1.upper()} vs {country2.upper()}")
        print(f"📋 Metric: {metric_name}")
        print(f"{'='*50}")
        
        comparison_data = []
        for year in ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015', '2022']:
            if year in data1 and year in data2 and data1[year] is not None and data2[year] is not None:
                diff = data1[year] - data2[year]
                winner = f"🏆 {country1}" if diff > 0 else f"🏆 {country2}" if diff < 0 else "⚖️ Tie"
                comparison_data.append({
                    'year': year,
                    country1: data1[year],
                    country2: data2[year],
                    'difference': abs(diff),
                    'winner': winner
                })
        
        for comp in comparison_data:
            print(f"   {comp['year']}: {comp[country1]:.1f} vs {comp[country2]:.1f} | {comp['winner']}")
    
    def export_country_data(self, country_name, filename=None):
        """Export country data to CSV"""
        results, correct_name = self.query_country(country_name)
        
        if not results:
            print("❌ Cannot export - country not found.")
            return
        
        if not filename:
            filename = f"{correct_name.replace(' ', '_').lower()}_energy_data.csv"
        
        # Convert to DataFrame
        df_data = []
        for doc in results:
            row = {
                'country': doc['country'],
                'metric': doc['metric'],
                'unit': doc['unit'],
                'sector': doc['sector'],
                'sub_sector': doc['sub_sector']
            }
            # Add year data
            for year in range(2000, 2023):
                year_str = str(year)
                if year_str in doc:
                    row[year_str] = doc[year_str]
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        df.to_csv(filename, index=False)
        print(f"✅ Data exported to: {filename}")
    
    def show_database_stats(self):
        """Show database statistics"""
        total_countries = len(self.get_all_countries())
        total_metrics = len(self.get_all_metrics())
        total_documents = self.collection.count_documents({})
        
        print(f"\n📊 DATABASE STATISTICS:")
        print(f"   🌍 Countries: {total_countries}")
        print(f"   📈 Metrics: {total_metrics}")
        print(f"   📄 Total Documents: {total_documents}")
        print(f"   🔍 Available Metrics: {', '.join(self.get_all_metrics())}")

def main():
    tool = EnergyQueryTool()
    
    if not tool.connect():
        return
    
    print("\n" + "="*60)
    print("🌍 AFRICA ENERGY DATA QUERY TOOL")
    print("="*60)
    
    while True:
        print("\nOptions:")
        print("1. 🔍 Query Country Data")
        print("2. 🆚 Compare Two Countries")
        print("3. 📤 Export Country Data")
        print("4. 📊 Show Database Stats")
        print("5. 🌍 List All Countries")
        print("6. ❌ Exit")
        
        choice = input("\nChoose option (1-6): ").strip()
        
        if choice == '1':
            country = input("Enter country name: ").strip()
            results, correct_name = tool.query_country(country)
            
            if results:
                tool.display_country_data(results, correct_name)
                
                # Ask about export
                export = input("\nExport this data to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    tool.export_country_data(correct_name)
            else:
                print(f"❌ {correct_name}")
        
        elif choice == '2':
            country1 = input("Enter first country: ").strip()
            country2 = input("Enter second country: ").strip()
            print("\nAvailable metrics:", ", ".join(tool.get_all_metrics()))
            metric = input("Enter metric to compare: ").strip()
            tool.compare_countries(country1, country2, metric)
        
        elif choice == '3':
            country = input("Enter country name to export: ").strip()
            filename = input("Enter filename (or press Enter for auto): ").strip()
            if filename:
                tool.export_country_data(country, filename)
            else:
                tool.export_country_data(country)
        
        elif choice == '4':
            tool.show_database_stats()
        
        elif choice == '5':
            countries = tool.get_all_countries()
            print(f"\n🌍 Available Countries ({len(countries)}):")
            for i, country in enumerate(countries, 1):
                print(f"   {i:2d}. {country}")
        
        elif choice == '6':
            print("👋 Thank you for using the Africa Energy Query Tool!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")
    
    tool.disconnect()

if __name__ == "__main__":
    main()

