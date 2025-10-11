import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class ReportGenerator:
    def __init__(self):
        self.client = MongoClient(os.getenv('MONGO_URI'))
        self.db = self.client.africa_energy_db
        self.collection = self.db.energy_metrics
    
    def generate_comprehensive_report(self):
        """Generate a comprehensive energy development report"""
        print("📄 Generating comprehensive report...")
        
        report_lines = []
        
        # Header
        report_lines.append("AFRICA ENERGY DEVELOPMENT REPORT")
        report_lines.append("=" * 60)
        report_lines.append(f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}")
        report_lines.append("")
        
        # Executive Summary
        report_lines.append("EXECUTIVE SUMMARY")
        report_lines.append("-" * 40)
        report_lines.extend(self._generate_executive_summary())
        report_lines.append("")
        
        # Electricity Access Analysis
        report_lines.append("ELECTRICITY ACCESS ANALYSIS")
        report_lines.append("-" * 40)
        report_lines.extend(self._analyze_electricity_access())
        report_lines.append("")
        
        # Clean Cooking Analysis
        report_lines.append("CLEAN COOKING ACCESS ANALYSIS")
        report_lines.append("-" * 40)
        report_lines.extend(self._analyze_clean_cooking())
        report_lines.append("")
        
        # Regional Comparisons
        report_lines.append("REGIONAL COMPARISONS")
        report_lines.append("-" * 40)
        report_lines.extend(self._generate_regional_comparisons())
        report_lines.append("")
        
        # Progress Tracking
        report_lines.append("PROGRESS TRACKING (2000-2022)")
        report_lines.append("-" * 40)
        report_lines.extend(self._track_progress())
        report_lines.append("")
        
        # Recommendations
        report_lines.append("KEY RECOMMENDATIONS")
        report_lines.append("-" * 40)
        report_lines.extend(self._generate_recommendations())
        
        # Save report
        filename = f"Africa_Energy_Report_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        print(f"✅ Comprehensive report saved as '{filename}'")
        return report_lines
    
    def _generate_executive_summary(self):
        """Generate executive summary section"""
        summary = []
        
        # Get basic stats
        total_countries = len(self.collection.distinct("country"))
        total_metrics = len(self.collection.distinct("metric"))
        
        # Electricity stats
        elec_data = list(self.collection.find({"metric": "Electricity Access Rate"}))
        current_elec = [doc for doc in elec_data if '2022' in doc and doc['2022'] is not None]
        
        if current_elec:
            avg_elec = sum(doc['2022'] for doc in current_elec) / len(current_elec)
            full_access = sum(1 for doc in current_elec if doc['2022'] == 100)
        else:
            avg_elec = 0
            full_access = 0
        
        summary.append(f"This report analyzes energy access across {total_countries} African countries, ")
        summary.append(f"tracking {total_metrics} key development indicators from 2000 to 2022.")
        summary.append("")
        summary.append(f"• Average electricity access rate: {avg_elec:.1f}%")
        summary.append(f"• Countries with universal electricity access: {full_access}")
        summary.append(f"• Comprehensive data covering two decades of development")
        summary.append("")
        summary.append("The analysis reveals significant progress in energy access, though substantial")
        summary.append("challenges remain, particularly in clean cooking access and regional disparities.")
        
        return summary
    
    def _analyze_electricity_access(self):
        """Analyze electricity access trends"""
        analysis = []
        
        elec_df = self._get_dataframe("Electricity Access Rate")
        if elec_df.empty:
            return ["No electricity access data available."]
        
        # Find latest year
        year_columns = [col for col in elec_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
        if not year_columns:
            return ["No year data available for electricity access."]
        
        latest_year = max(year_columns)
        current_data = elec_df[['country', latest_year]].dropna()
        
        analysis.append(f"CURRENT STATUS ({latest_year}):")
        analysis.append(f"  • Countries analyzed: {len(current_data)}")
        analysis.append(f"  • Average access rate: {current_data[latest_year].mean():.1f}%")
        analysis.append(f"  • Universal access (100%): {len(current_data[current_data[latest_year] == 100])} countries")
        analysis.append(f"  • Critical gap (<50%): {len(current_data[current_data[latest_year] < 50])} countries")
        analysis.append("")
        
        # Regional leaders
        analysis.append("REGIONAL LEADERS:")
        top_5 = current_data.nlargest(5, latest_year)
        for _, row in top_5.iterrows():
            analysis.append(f"  • {row['country']}: {row[latest_year]}%")
        analysis.append("")
        
        # Most improved
        if '2000' in elec_df.columns:
            improvements = []
            for _, row in elec_df.iterrows():
                if pd.notna(row.get('2000')) and pd.notna(row.get(latest_year)):
                    improvement = row[latest_year] - row['2000']
                    improvements.append({
                        'country': row['country'],
                        'improvement': improvement
                    })
            
            if improvements:
                improvements_df = pd.DataFrame(improvements).nlargest(3, 'improvement')
                analysis.append("MOST IMPROVED (2000-2022):")
                for _, row in improvements_df.iterrows():
                    analysis.append(f"  • {row['country']}: +{row['improvement']:.1f}%")
        
        return analysis
    
    def _analyze_clean_cooking(self):
        """Analyze clean cooking access trends"""
        analysis = []
        
        cook_df = self._get_dataframe("Clean Cooking Access Rate")
        if cook_df.empty:
            return ["No clean cooking data available."]
        
        # Find latest year
        year_columns = [col for col in cook_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
        if not year_columns:
            return ["No year data available for clean cooking."]
        
        latest_year = max(year_columns)
        current_data = cook_df[['country', latest_year]].dropna()
        
        analysis.append(f"CURRENT STATUS ({latest_year}):")
        analysis.append(f"  • Average access rate: {current_data[latest_year].mean():.1f}%")
        analysis.append(f"  • High access (>90%): {len(current_data[current_data[latest_year] > 90])} countries")
        analysis.append(f"  • Critical gap (<10%): {len(current_data[current_data[latest_year] < 10])} countries")
        analysis.append("")
        analysis.append("Clean cooking access remains a significant challenge across much of Africa,")
        analysis.append("with many countries showing limited progress compared to electricity access.")
        
        return analysis
    
    def _generate_regional_comparisons(self):
        """Generate regional comparisons"""
        comparisons = []
        
        # Simple regional grouping (you could enhance this with actual regions)
        regions = {
            'North Africa': ['Egypt', 'Libya', 'Tunisia', 'Algeria', 'Morocco'],
            'West Africa': ['Nigeria', 'Ghana', 'Ivory Coast', 'Senegal', 'Mali'],
            'East Africa': ['Kenya', 'Tanzania', 'Ethiopia', 'Uganda', 'Rwanda'],
            'Southern Africa': ['South Africa', 'Namibia', 'Botswana', 'Zimbabwe', 'Zambia']
        }
        
        elec_df = self._get_dataframe("Electricity Access Rate")
        if elec_df.empty:
            return ["No data available for regional comparisons."]
        
        year_columns = [col for col in elec_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
        if not year_columns:
            return ["No year data available for regional comparisons."]
        
        latest_year = max(year_columns)
        
        comparisons.append(f"REGIONAL ELECTRICITY ACCESS ({latest_year}):")
        
        for region, countries in regions.items():
            region_data = elec_df[elec_df['country'].isin(countries)]
            if not region_data.empty:
                region_current = region_data[['country', latest_year]].dropna()
                if not region_current.empty:
                    avg_access = region_current[latest_year].mean()
                    comparisons.append(f"  • {region}: {avg_access:.1f}%")
        
        comparisons.append("")
        comparisons.append("North African countries generally show higher access rates,")
        comparisons.append("while significant disparities exist within other regions.")
        
        return comparisons
    
    def _track_progress(self):
        """Track progress over time"""
        progress = []
        
        elec_df = self._get_dataframe("Electricity Access Rate")
        if elec_df.empty or '2000' not in elec_df.columns:
            return ["Insufficient data for progress tracking."]
        
        year_columns = [col for col in elec_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
        if not year_columns:
            return ["No year data available for progress tracking."]
        
        latest_year = max(year_columns)
        
        # Calculate overall progress
        improvements = []
        for _, row in elec_df.iterrows():
            if pd.notna(row.get('2000')) and pd.notna(row.get(latest_year)):
                improvements.append(row[latest_year] - row['2000'])
        
        if improvements:
            avg_improvement = sum(improvements) / len(improvements)
            progress.append(f"OVERALL PROGRESS (2000-{latest_year}):")
            progress.append(f"  • Average improvement: +{avg_improvement:.1f}%")
            progress.append(f"  • Countries showing progress: {len([x for x in improvements if x > 0])}")
            progress.append(f"  • Countries with >20% improvement: {len([x for x in improvements if x > 20])}")
        
        return progress
    
    def _generate_recommendations(self):
        """Generate key recommendations"""
        recommendations = []
        
        recommendations.append("1. FOCUS ON CLEAN COOKING ACCESS")
        recommendations.append("   • Clean cooking progress lags significantly behind electricity access")
        recommendations.append("   • Targeted programs needed for countries with <10% access")
        recommendations.append("")
        
        recommendations.append("2. ADDRESS REGIONAL DISPARITIES")
        recommendations.append("   • Significant gaps exist between North Africa and other regions")
        recommendations.append("   • Regional cooperation could accelerate progress")
        recommendations.append("")
        
        recommendations.append("3. LEVERAGE SUCCESS STORIES")
        recommendations.append("   • Study and replicate strategies from most improved countries")
        recommendations.append("   • Share best practices across the continent")
        recommendations.append("")
        
        recommendations.append("4. ENHANCE DATA COLLECTION")
        recommendations.append("   • Regular monitoring essential for tracking SDG progress")
        recommendations.append("   • Invest in national statistical capacity")
        
        return recommendations
    
    def _get_dataframe(self, metric_name):
        """Helper function to get DataFrame for specific metric"""
        data = list(self.collection.find({"metric": metric_name}))
        
        df_data = []
        for doc in data:
            row = {'country': doc['country']}
            for year in range(2000, 2023):
                year_str = str(year)
                if year_str in doc:
                    row[year_str] = doc[year_str]
            df_data.append(row)
        
        return pd.DataFrame(df_data)
    
    def generate_quick_report(self):
        """Generate a quick one-page summary report"""
        print("📋 Generating quick summary report...")
        
        report = []
        report.append("AFRICA ENERGY QUICK REPORT")
        report.append("=" * 40)
        report.append(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
        report.append("")
        
        # Key statistics
        countries = self.collection.distinct("country")
        report.append(f"• Countries covered: {len(countries)}")
        report.append(f"• Time period: 2000-2022")
        report.append("")
        
        # Top 3 electricity access
        elec_df = self._get_dataframe("Electricity Access Rate")
        if not elec_df.empty:
            year_columns = [col for col in elec_df.columns if col.isdigit() and 2000 <= int(col) <= 2023]
            if year_columns:
                latest_year = max(year_columns)
                current_data = elec_df[['country', latest_year]].dropna()
                top_3 = current_data.nlargest(3, latest_year)
                
                report.append("TOP 3 - ELECTRICITY ACCESS:")
                for _, row in top_3.iterrows():
                    report.append(f"  {row['country']}: {row[latest_year]}%")
        
        filename = "Africa_Energy_Quick_Report.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"✅ Quick report saved as '{filename}'")
        return report

def main():
    generator = ReportGenerator()
    
    print("\n" + "="*60)
    print("📄 AFRICA ENERGY REPORT GENERATOR")
    print("="*60)
    
    while True:
        print("\nReport Options:")
        print("1. 📊 Generate Comprehensive Report")
        print("2. 📋 Generate Quick Summary")
        print("3. ❌ Exit")
        
        choice = input("\nChoose option (1-3): ").strip()
        
        if choice == '1':
            generator.generate_comprehensive_report()
        elif choice == '2':
            generator.generate_quick_report()
        elif choice == '3':
            print("👋 Thank you for using the Report Generator!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

