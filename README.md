# Africa Energy Data Pipeline


An automated ETL (Extract, Transform, Load) system that processes electricity access data for all 54 African countries from 2000 to 2022 and stores it in MongoDB for analysis and reporting.


## 🚀 Features


- **Data Extraction**: Reads energy metrics from CSV datasets
 
- **Data Transformation**: Converts raw data into structured MongoDB documents
 
- **Data Validation**: Ensures data quality with 92% completeness score
 
- **MongoDB Integration**: Stores data with optimized indexing and schema
 
- **Secure Configuration**: Uses environment variables for sensitive credentials
 


## 📊 Data Coverage


- **54 African countries** fully covered
 
- **23 years** of data (2000-2022)
 
- **Metric**: Electricity Access Rate (% of population)
 
- **Data Completeness**: 92%
 


## 🛠️ Tech Stack


- **Python 3.x**
 
- **Pandas** - Data processing
 
- **MongoDB** - Database
 
- **PyMongo** - MongoDB driver
 
- **python-dotenv** - Environment management
 


## 📁 Project Structure

energy_data_etraction/

├── energy_data_processor.py # Main processing script

├── Database_Electricity.csv # Source data

├── .env # Environment variables

├── requirements.txt # Dependencies

└── README.md # This file




## ⚡ Quick Start


1. **Install dependencies**:

 
   ```bash
   
   pip install -r requirements.txt
   
Configure environment:

Create .env file with:


env

MONGO_URI=your_mongodb_connection_string

CSV_FILE_PATH=path_to_your_csv_file

Run the pipeline:


bash

python energy_data_processor.py

📈 Output Schema

Each MongoDB document contains:


json

{

  "country": "Nigeria",
  
  "country_serial": 39,
  
  "metric": "Electricity Access Rate",
  
  "unit": "%",
  
  "sector": "Power",
  
  "sub_sector": "Access",
  
  "sub_sub_sector": "National",
  
  "source_link": "CSV File: Database_Electricity.csv",
  
  "source": "Africa Energy Portal Dataset",
  
  "2000": 43.2,
  
  "2001": 43.9,
  
  ...
  
  "2022": 60.5
  
}

🎯 Use Cases

Energy access trend analysis


African development reporting


Data visualization dashboards


Research and academic studies


Policy decision support



Built for Africa Energy Analytics • Data for Development 🌍



This README is professional, concise, and perfect for showcasing your project! It includes all the essential sections that employers and collaborators look for.
