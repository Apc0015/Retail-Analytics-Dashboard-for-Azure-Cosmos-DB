# Retail Analytics Dashboard for Azure Cosmos DB

A comprehensive retail analytics dashboard built with Streamlit and Azure Cosmos DB (MongoDB API), providing real-time insights into products, customers, orders, and reviews.

**Author**: Ayush Chhoker - U00363568

## Features

- **Product Analysis**: Track inventory, pricing, and product performance across categories
- **Customer Insights**: Monitor customer distribution, loyalty tiers, and geographic trends
- **Order Analytics**: Analyze sales patterns, revenue metrics, and payment preferences
- **Review Monitoring**: Evaluate customer satisfaction and product ratings

## Setup

### Prerequisites

- Python 3.8+
- Azure Cosmos DB account with MongoDB API
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Apc0015/Retail-Analytics-Dashboard-for-Azure-Cosmos-DB.git
cd Retail-Analytics-Dashboard-for-Azure-Cosmos-DB
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env and add your Azure Cosmos DB connection string
```

### Configuration

Add your Azure Cosmos DB connection string to the `.env` file:
```
COSMOS_CONNECTION_STRING=your_connection_string_here
```

Or set it as an environment variable:
```bash
export COSMOS_CONNECTION_STRING="your_connection_string_here"
```

## Usage

### Upload Sample Data (Optional)

To populate your database with sample data:
```bash
python Data_Uploading.py
```

### Run the Dashboard

```bash
streamlit run dashboard_retail.py
```

The dashboard will open in your default web browser at `http://localhost:8501`

## Project Structure

```
├── dashboard_retail.py      # Main Streamlit dashboard application
├── Data_Uploading.py        # Script to generate and upload sample data
├── requirements.txt         # Python dependencies
├── Solution.ipynb          # Jupyter notebook with analysis
├── .env.example            # Environment variables template
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## Technologies

- **Frontend**: Streamlit
- **Database**: Azure Cosmos DB (MongoDB API)
- **Visualization**: Plotly
- **Data Processing**: Pandas, NumPy

## Security

⚠️ **Important**: Never commit your `.env` file or expose your Azure Cosmos DB connection string in public repositories.

## License

This project is for educational purposes.
