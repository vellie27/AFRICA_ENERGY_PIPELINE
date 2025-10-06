# Africa Energy Data Pipeline

An automated ETL (Extract, Transform, Load) system that processes multiple energy metrics for all 54 African countries from 2000 to 2022 and stores them in MongoDB for comprehensive energy analysis.

## 🚀 Features

- **Multi-Metric Processing**: Handles 4 different energy indicators automatically
- **Auto-File Detection**: Automatically finds your data file in the directory
- **Data Validation**: Comprehensive quality checks with completeness scoring
- **MongoDB Integration**: Optimized storage with proper indexing
- **Smart Error Handling**: Clear guidance for troubleshooting

## 📊 Energy Metrics Processed

1. **Electricity Access Rate** (% of population)
2. **Clean Cooking Access Rate** (% of population) 
3. **Clean Cooking Access Gap** (millions without access)
4. **Energy Intensity** (MJ/2017 PPP GDP)

## 🛠️ Tech Stack

- **Python 3.x**
- **Pandas** - Data processing
- **MongoDB** - NoSQL database
- **PyMongo** - MongoDB driver
- **python-dotenv** - Secure configuration

## 📁 Project Structure

ENERGY_DATA_ETRACTION/

├── energy_data_processor.py # Main processing script

├── Database_Electricity.csv # Your energy data file

├── .env # Environment configuration

├── requirements.txt # Python dependencies

└── README.md # This file


## ⚡ Quick Start

### 1. Installation
```bash
# Install dependencies
pip install -r requirements.txt

