import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Try to import visualization packages
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALS_AVAILABLE = True
except ImportError:
    VISUALS_AVAILABLE = False
    print("⚠️  Visualization packages not available. Running in text-only mode.")

load_dotenv()

class EnergyAnalysisDashboard:
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
            
            # Check if collection has data
            count = self.collection.count_documents({})
            print(f"📊 Documents in database: {count}")
            
            if count == 0:
                print("❌ Database is empty! Please run your data processor first.")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.client:
            self.client.close()
    
    def check_database_status(self):
        """Check what's actually in the database"""
        print("\n" + "="*60)
        print("🔍 DATABASE STATUS CHECK")
        print("="*60)
        
        # Count total documents
        total_docs = self.collection.count_documents({})
        print(f"Total documents: {total_docs}")
        
        # Get available countries
        countries = self.collection.distinct("country")
        print(f"Countries in database: {len(countries)}")
        if countries:
            print(f"Sample countries: {', '.join(countries[:5])}")
        
        # Get available metrics
        metrics = self.collection.distinct("metric")
        print(f"Metrics in database: {len(metrics)}")
        if metrics:
            print(f"Available metrics: {', '.join(metrics)}")
        
        # Show sample document structure
        sample = self.collection.find_one()
        if sample:
            print(f"\n📄 Sample document fields:")
            for key in list(sample.keys())[:10]:  # Show first 10 fields
                print(f"  • {key}: {type(sample[key]).__name__}")
        
        return total_docs > 0
    
    def safe_get_dataframe(self, metric_name=None):
        """Safely convert MongoDB data to pandas DataFrame with error handling"""
        try:
            query = {} if metric_name is None else {"metric": metric_name}
            data = list(self.collection.find(query))
            
            if not data:
                print(f"⚠️  No data found for metric: {metric_name}")
                return pd.DataFrame()
            
            df_data = []
            for doc in data:
                row = {
                    'country': doc.get('country', 'Unknown'),
                    'metric': doc.get('metric', 'Unknown'),
                    'unit': doc.get('unit', 'Unknown'),
                    'sector': doc.get('sector', 'Unknown')
                }
                # Safely add year data
                for year in range(2000, 2023):
                    year_str = str(year)
                    if year_str in doc:
                        row[year_str] = doc[year_str]
                df_data.append(row)
            
            df = pd.DataFrame(df_data)
            print(f"✅ Loaded {len(df)} records for {metric_name or 'all metrics'}")
            return df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    
    def analyze_electricity_access(self):
        """Safely analyze electricity access trends"""
        print("\n" + "="*60)
        print("⚡ ELECTRICITY ACCESS ANALYSIS")
        print("="*60)
        
        df = self.safe_get_dataframe("Electricity Access Rate")
        
        if df.empty:
            print("❌ No electricity access data available.")
            return df
        
        # Check if we have 2022 data
        if '2022' not in df.columns:
            print("⚠️  No 2022 data available. Using most recent year...")
            # Find the most recent year with data
            year_columns = [col for col in df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
            if not year_columns:
                print("❌ No year data available.")
                return df
            latest_year = max(year_columns)
            print(f"📅 Using data from {latest_year}")
            year_column = latest_year
        else:
            year_column = '2022'
        
        # Current Status
        current_data = df[['country', year_column]].dropna()
        
        if current_data.empty:
            print("❌ No current data available for analysis.")
            return df
        
        current_data = current_data.sort_values(year_column, ascending=False)
        
        print(f"\n📊 CURRENT STATUS ({year_column}):")
        print(f"   • Countries analyzed: {len(current_data)}")
        print(f"   • Countries with 100% access: {len(current_data[current_data[year_column] == 100])}")
        print(f"   • Countries below 50% access: {len(current_data[current_data[year_column] < 50])}")
        print(f"   • Average access rate: {current_data[year_column].mean():.1f}%")
        print(f"   • Median access rate: {current_data[year_column].median():.1f}%")
        
        # Top Performers
        print(f"\n🏆 TOP 5 COUNTRIES ({year_column}):")
        top_5 = current_data.head(5)
        for _, row in top_5.iterrows():
            print(f"   • {row['country']}: {row[year_column]}%")
        
        # Bottom Performers
        print(f"\n📉 BOTTOM 5 COUNTRIES ({year_column}):")
        bottom_5 = current_data.tail(5)
        for _, row in bottom_5.iterrows():
            print(f"   • {row['country']}: {row[year_column]}%")
        
        # Biggest Improvements (if we have 2000 data)
        if '2000' in df.columns:
            improvements = []
            for _, row in df.iterrows():
                if pd.notna(row.get('2000')) and pd.notna(row.get(year_column)):
                    improvement = row[year_column] - row['2000']
                    improvements.append({
                        'country': row['country'],
                        'improvement': improvement,
                        '2000': row['2000'],
                        'latest': row[year_column]
                    })
            
            if improvements:
                improvements_df = pd.DataFrame(improvements).sort_values('improvement', ascending=False)
                print(f"\n📈 BIGGEST IMPROVEMENTS (2000-{year_column}):")
                top_improvers = improvements_df.head(5)
                for _, row in top_improvers.iterrows():
                    print(f"   • {row['country']}: +{row['improvement']:.1f}% ({row['2000']}% → {row['latest']}%)")
        
        return df
    
    def analyze_clean_cooking(self):
        """Safely analyze clean cooking access trends"""
        print("\n" + "="*60)
        print("🔥 CLEAN COOKING ACCESS ANALYSIS")
        print("="*60)
        
        df = self.safe_get_dataframe("Clean Cooking Access Rate")
        
        if df.empty:
            print("❌ No clean cooking data available.")
            return df
        
        # Find latest year with data
        year_columns = [col for col in df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
        if not year_columns:
            print("❌ No year data available.")
            return df
        
        latest_year = max(year_columns)
        year_column = '2022' if '2022' in df.columns else latest_year
        
        # Current status
        current_data = df[['country', year_column]].dropna()
        
        if current_data.empty:
            print("❌ No current data available for analysis.")
            return df
        
        print(f"\n📊 CLEAN COOKING ACCESS ({year_column}):")
        print(f"   • Countries analyzed: {len(current_data)}")
        print(f"   • Countries with >90% access: {len(current_data[current_data[year_column] > 90])}")
        print(f"   • Countries with <10% access: {len(current_data[current_data[year_column] < 10])}")
        print(f"   • Average access rate: {current_data[year_column].mean():.1f}%")
        
        # Top Performers
        print(f"\n🏆 TOP 5 COUNTRIES ({year_column}):")
        top_5 = current_data.nlargest(5, year_column)
        for _, row in top_5.iterrows():
            print(f"   • {row['country']}: {row[year_column]}%")
        
        return df
    
    def create_safe_visualizations(self):
        """Safely create visualizations with comprehensive error handling"""
        if not VISUALS_AVAILABLE:
            print("⚠️  Visualization packages not available. Skipping charts.")
            return
        
        print("\n" + "="*60)
        print("📈 GENERATING VISUALIZATIONS")
        print("="*60)
        
        try:
            # Get electricity data
            df = self.safe_get_dataframe("Electricity Access Rate")
            if df.empty:
                print("❌ No electricity data available for visualization.")
                return
            
            # Find latest year with data
            year_columns = [col for col in df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
            if not year_columns:
                print("❌ No year data available for visualization.")
                return
            
            latest_year = max(year_columns)
            year_column = '2022' if '2022' in df.columns else latest_year
            
            current_data = df[['country', year_column]].dropna()
            if current_data.empty:
                print("❌ No current data available for visualization.")
                return
            
            # Create simple bar chart
            top_10 = current_data.nlargest(10, year_column)
            
            plt.figure(figsize=(12, 8))
            plt.barh(top_10['country'], top_10[year_column], color='skyblue')
            plt.xlabel(f'Electricity Access Rate (%) - {year_column}')
            plt.title(f'Top 10 African Countries - Electricity Access ({year_column})')
            plt.gca().invert_yaxis()  # Highest at top
            plt.tight_layout()
            
            # Save the chart
            filename = f'electricity_top10_{year_column}.png'
            plt.savefig(filename, dpi=150, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Chart saved as '{filename}'")
            
        except Exception as e:
            print(f"❌ Error creating visualization: {e}")
            print("💡 This might be due to missing data or visualization issues.")
    
    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("\n" + "="*60)
        print("📄 GENERATING SUMMARY REPORT")
        print("="*60)
        
        report_lines = []
        report_lines.append("AFRICA ENERGY DEVELOPMENT SUMMARY REPORT")
        report_lines.append("=" * 50)
        report_lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
        report_lines.append("")
        
        # Get database stats
        total_docs = self.collection.count_documents({})
        countries = self.collection.distinct("country")
        metrics = self.collection.distinct("metric")
        
        report_lines.append("📊 DATABASE OVERVIEW")
        report_lines.append(f"• Total documents: {total_docs}")
        report_lines.append(f"• Countries covered: {len(countries)}")
        report_lines.append(f"• Metrics tracked: {len(metrics)}")
        report_lines.append(f"• Available metrics: {', '.join(metrics)}")
        report_lines.append("")
        
        # Electricity Analysis
        elec_df = self.safe_get_dataframe("Electricity Access Rate")
        if not elec_df.empty:
            year_columns = [col for col in elec_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
            if year_columns:
                latest_year = max(year_columns)
                current_elec = elec_df[['country', latest_year]].dropna()
                
                report_lines.append("⚡ ELECTRICITY ACCESS SUMMARY")
                report_lines.append(f"• Countries analyzed: {len(current_elec)}")
                report_lines.append(f"• Average access rate ({latest_year}): {current_elec[latest_year].mean():.1f}%")
                report_lines.append(f"• Countries with 100% access: {len(current_elec[current_elec[latest_year] == 100])}")
                report_lines.append("")
                
                # Top 3 countries
                top_3 = current_elec.nlargest(3, latest_year)
                report_lines.append("🏆 TOP 3 COUNTRIES:")
                for _, row in top_3.iterrows():
                    report_lines.append(f"  • {row['country']}: {row[latest_year]}%")
        
        # Save report
        report_filename = 'energy_analysis_report.txt'
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        print(f"✅ Summary report saved as '{report_filename}'")
        return report_lines
    
    def run_complete_analysis(self):
        """Run all analysis components safely"""
        print("\n🌍 STARTING COMPREHENSIVE ENERGY ANALYSIS")
        print("="*60)
        
        # First check database status
        if not self.check_database_status():
            print("❌ Cannot proceed - database issues detected.")
            return
        
        # Run analyses
        electricity_data = self.analyze_electricity_access()
        cooking_data = self.analyze_clean_cooking()
        
        # Generate visualizations
        self.create_safe_visualizations()
        
        # Generate report
        self.generate_summary_report()
        
        print("\n🎉 ANALYSIS COMPLETE!")
    
    def list_all_data(self):
        """List all data in the database"""
        print("\n" + "="*60)
        print("📋 ALL DATA IN DATABASE")
        print("="*60)
        
        all_data = list(self.collection.find().limit(20))  # Limit to first 20 documents
        
        for i, doc in enumerate(all_data, 1):
            print(f"\n📄 Document {i}:")
            print(f"   Country: {doc.get('country', 'N/A')}")
            print(f"   Metric: {doc.get('metric', 'N/A')}")
            print(f"   Unit: {doc.get('unit', 'N/A')}")
            
            # Show available years with data
            year_data = []
            for year in range(2000, 2023):
                year_str = str(year)
                if year_str in doc and doc[year_str] is not None:
                    year_data.append(f"{year_str}: {doc[year_str]}")
            
            if year_data:
                print(f"   Data: {', '.join(year_data[:5])}")  # Show first 5 years

def main():
    dashboard = EnergyAnalysisDashboard()
    
    if not dashboard.connect():
        print("❌ Failed to connect to database. Exiting.")
        return
    
    print("\n" + "="*60)
    print("📊 AFRICA ENERGY ANALYSIS DASHBOARD")
    print("="*60)
    
    if not VISUALS_AVAILABLE:
        print("⚠️  Running in text-only mode (visualizations disabled)")
    
    while True:
        print("\nAnalysis Options:")
        print("1. 🔍 Run Complete Analysis")
        print("2. ⚡ Analyze Electricity Access")
        print("3. 🔥 Analyze Clean Cooking Access") 
        print("4. 📈 Generate Visualizations")
        print("5. 📄 Generate Report")
        print("6. 📋 List All Data (Debug)")
        print("7. 🔎 Check Database Status")
        print("8. ❌ Exit")
        
        choice = input("\nChoose option (1-8): ").strip()
        
        if choice == '1':
            dashboard.run_complete_analysis()
        elif choice == '2':
            dashboard.analyze_electricity_access()
        elif choice == '3':
            dashboard.analyze_clean_cooking()
        elif choice == '4':
            dashboard.create_safe_visualizations()
        elif choice == '5':
            dashboard.generate_summary_report()
        elif choice == '6':
            dashboard.list_all_data()
        elif choice == '7':
            dashboard.check_database_status()
        elif choice == '8':
            print("👋 Thank you for using the Energy Analysis Dashboard!")
            break
        else:
            print("❌ Invalid choice. Please try again.")
    
    dashboard.disconnect()

if __name__ == "__main__":
    main()

