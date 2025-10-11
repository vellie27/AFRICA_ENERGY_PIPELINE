import os
import pandas as pd
import json
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

class DataExporter:
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGO_URI'))
        self.db = self.client.africa_energy_db
        self.collection = self.db.energy_metrics
    
    def export_all_to_csv(self, filename="all_energy_data.csv"):
        """Export all data to a single CSV file"""
        print(f"📤 Exporting all data to {filename}...")
        
        all_data = list(self.collection.find())
        df_data = []
        
        for doc in all_data:
            row = {
                'country': doc['country'],
                'country_serial': doc['country_serial'],
                'metric': doc['metric'],
                'unit': doc['unit'],
                'sector': doc['sector'],
                'sub_sector': doc['sub_sector'],
                'sub_sub_sector': doc['sub_sub_sector'],
                'source': doc['source']
            }
            # Add year data
            for year in range(2000, 2023):
                year_str = str(year)
                if year_str in doc:
                    row[year_str] = doc[year_str]
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        df.to_csv(filename, index=False)
        print(f"✅ All data exported to {filename}")
        return df
    
    def export_by_country(self, country_name=None):
        """Export data for specific country or all countries individually"""
        if country_name:
            # Export single country
            results = list(self.collection.find({
                "country": {"$regex": f"^{country_name}$", "$options": "i"}
            }))
            
            if results:
                filename = f"{country_name.replace(' ', '_').lower()}_energy_data.csv"
                self._export_country_data(results, filename)
            else:
                print(f"❌ Country '{country_name}' not found")
        else:
            # Export all countries individually
            countries = self.collection.distinct("country")
            for country in countries:
                results = list(self.collection.find({"country": country}))
                filename = f"exports/{country.replace(' ', '_').lower()}_energy_data.csv"
                self._export_country_data(results, filename)
    
    def _export_country_data(self, data, filename):
        """Helper function to export country data"""
        df_data = []
        for doc in data:
            row = {
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
        
        # Create exports directory if it doesn't exist
        os.makedirs('exports', exist_ok=True)
        
        df.to_csv(filename, index=False)
        print(f"✅ Exported {len(data)} metrics to {filename}")
    
    def export_for_tableau(self, filename="tableau_energy_data.csv"):
        """Export data formatted for Tableau analysis"""
        print(f"📊 Exporting Tableau-ready data to {filename}...")
        
        all_data = list(self.collection.find())
        tableau_data = []
        
        for doc in all_data:
            for year in range(2000, 2023):
                year_str = str(year)
                if year_str in doc and doc[year_str] is not None:
                    tableau_data.append({
                        'country': doc['country'],
                        'country_serial': doc['country_serial'],
                        'metric': doc['metric'],
                        'unit': doc['unit'],
                        'sector': doc['sector'],
                        'sub_sector': doc['sub_sector'],
                        'year': int(year_str),
                        'value': doc[year_str],
                        'source': doc['source']
                    })
        
        df = pd.DataFrame(tableau_data)
        df.to_csv(filename, index=False)
        print(f"✅ Tableau data exported to {filename} ({len(df)} rows)")
        return df
    
    def export_to_json(self, filename="energy_data.json"):
        """Export data to JSON format for web applications"""
        print(f"🌐 Exporting JSON data to {filename}...")
        
        all_data = list(self.collection.find())
        
        # Remove MongoDB _id field
        for doc in all_data:
            doc.pop('_id', None)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ JSON data exported to {filename}")
    
    def export_to_excel(self, filename="energy_data_analysis.xlsx"):
        """Export data to Excel with multiple sheets"""
        print(f"📗 Exporting Excel workbook to {filename}...")
        
        # Create Excel writer
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Export each metric to separate sheet
            metrics = self.collection.distinct("metric")
            
            for metric in metrics:
                data = list(self.collection.find({"metric": metric}))
                df_data = []
                
                for doc in data:
                    row = {'country': doc['country']}
                    # Add year data
                    for year in range(2000, 2023):
                        year_str = str(year)
                        if year_str in doc:
                            row[year_str] = doc[year_str]
                    df_data.append(row)
                
                df = pd.DataFrame(df_data)
                # Shorten sheet name if too long
                sheet_name = metric[:31]  # Excel limit
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Excel workbook exported to {filename}")
    
    def run_all_exports(self):
        """Run all export functions"""
        print("🚀 STARTING COMPREHETE DATA EXPORT")
        print("=" * 50)
        
        self.export_all_to_csv()
        self.export_for_tableau()
        self.export_to_json()
        self.export_to_excel()
        self.export_by_country()  # Export all countries individually
        
        print("\n🎉 ALL EXPORTS COMPLETED!")
        print("📁 Generated files:")
        print("   • all_energy_data.csv - Complete dataset")
        print("   • tableau_energy_data.csv - Tableau-formatted data")
        print("   • energy_data.json - JSON for web apps")
        print("   • energy_data_analysis.xlsx - Excel workbook")
        print("   • exports/ folder - Individual country files")

def main():
    exporter = DataExporter()
    
    print("\n" + "="*60)
    print("📤 AFRICA ENERGY DATA EXPORTER")
    print("="*60)
    
    while True:
        print("\nExport Options:")
        print("1. 📥 Run All Exports")
        print("2. 📊 Export for Tableau")
        print("3. 🌐 Export for Web (JSON)")
        print("4. 📗 Export for Excel")
        print("5. 🇰🇪 Export Single Country")
        print("6. 🌍 Export All Countries Individually")
        print("7. ❌ Exit")
        
        choice = input("\nChoose option (1-7): ").strip()
        
        if choice == '1':
            exporter.run_all_exports()
        elif choice == '2':
            exporter.export_for_tableau()
        elif choice == '3':
            exporter.export_to_json()
        elif choice == '4':
            exporter.export_to_excel()
        elif choice == '5':
            country = input("Enter country name: ").strip()
            exporter.export_by_country(country)
        elif choice == '6':
            exporter.export_by_country()
        elif choice == '7':
            print("👋 Thank you for using the Data Exporter!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

