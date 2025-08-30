#!/usr/bin/env python
# coding: utf-8

# In[12]:


#!/usr/bin/env python3
"""
Your First Data Collection Script
Let's start simple and build up!
"""


# In[1]:


import sys
print (sys.executable)


# In[6]:


import pandas as pd
print(pd.__version__)


# In[11]:


import os
import pandas as pd
import requests
import json
from datetime import datetime
from pathlib import Path
import time


# In[14]:


# STEP 1: Set up your paths
# This ensures your script works from any directory
# Option 1: Use current working directory
BASE_DIR = Path.cwd()  # Gets current working directory

# Option 2: If you want to specify exact path, uncomment and modify:
# BASE_DIR = Path("/your/exact/path/to/infrastructure-climate-analytics")

# Option 3: Use relative path from where you run the script
# BASE_DIR = Path(".")


# In[15]:


# Set up data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"


# In[16]:


# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


# In[17]:


print(f"📁 Working directory: {BASE_DIR}")
print(f"📁 Data will be saved to: {RAW_DATA_DIR}")


# In[21]:


def download_world_bank_indicator(indicator_code, indicator_name):
    """
    Download a specific indicator from World Bank API
    
    Args:
        indicator_code: World Bank indicator code (e.g., 'EN.ATM.CO2E.PC')
        indicator_name: Friendly name for saving the file
    """
    
    print(f"\n📊 Downloading {indicator_name}...")
    
    # World Bank API endpoint
    base_url = "https://api.worldbank.org/v2/country/all/indicator"
    
    # Build the full URL
    url = f"{base_url}/{indicator_code}"
    
    # Parameters for the API call
    params = {
        'format': 'json',
        'date': '2010:2023',  # Years we want
        'per_page': '5000'    # Get all results in one page
    }
    
    try:
        # Make the API request
        print(f"   🔍 Fetching from: {url}")
        response = requests.get(url, params=params)
        
        # Check if request was successful
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()
            
            # World Bank returns data in second element of list
            if len(data) > 1 and data[1]:
                # Convert to DataFrame for easier handling
                df = pd.DataFrame(data[1])
                
                # Save as CSV for easy viewing
                output_file = RAW_DATA_DIR / f"worldbank_{indicator_name}.csv"
                df.to_csv(output_file, index=False)
                
                print(f"   ✅ Saved {len(df)} rows to {output_file.name}")
                return df
            else:
                print(f"   ⚠️  No data found for {indicator_name}")
                return None
        else:
            print(f"   ❌ Error: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   ❌ Failed to download {indicator_name}: {e}")
        return None


# In[24]:


def download_sample_infrastructure_data():
    """
    Create sample infrastructure data
    Since real infrastructure APIs often require keys, we'll create sample data
    """
    
    print("\n🏗️ Creating sample infrastructure resilience data...")
    
    # Sample data representing infrastructure resilience scores
    countries = [
        'United States', 'China', 'Japan', 'Germany', 'India', 
        'United Kingdom', 'France', 'Italy', 'Brazil', 'Canada',
        'South Korea', 'Spain', 'Australia', 'Mexico', 'Indonesia'
    ]
    
    years = list(range(2010, 2024))
    
    # Create sample data
    data = []
    for country in countries:
        for year in years:
            # Simulate improving infrastructure scores over time
            base_score = 50 + (countries.index(country) * 2)
            year_improvement = (year - 2010) * 0.5
            
            data.append({
                'country': country,
                'year': year,
                'infrastructure_score': base_score + year_improvement,
                'transport_resilience': base_score + year_improvement + 5,
                'energy_resilience': base_score + year_improvement - 5,
                'water_resilience': base_score + year_improvement + 2,
                'digital_resilience': base_score + year_improvement + 10
            })
    
    df = pd.DataFrame(data)
    
    # Save to CSV
    output_file = RAW_DATA_DIR / "infrastructure_resilience_scores.csv"
    df.to_csv(output_file, index=False)
    
    print(f"   ✅ Created sample data with {len(df)} rows")
    print(f"   📄 Saved to {output_file.name}")
    
    return df


# In[25]:


def create_data_catalog():
    """
    Create a catalog of all downloaded data
    This helps track what data you have and when it was downloaded
    """
    
    print("\n📚 Creating data catalog...")
    
    catalog = []
    
    # Check all CSV files in raw data directory
    for file in RAW_DATA_DIR.glob("*.csv"):
        # Get file info
        file_stats = file.stat()
        
        # Read first few rows to get info
        try:
            df_sample = pd.read_csv(file, nrows=5)
            total_rows = len(pd.read_csv(file))
        except Exception as e:
            print(f"   ⚠️ Could not read {file.name}: {e}")
            continue
        
        catalog.append({
            'filename': file.name,
            'rows': total_rows,
            'columns': len(df_sample.columns),
            'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
            'downloaded': datetime.fromtimestamp(file_stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        })
    
    if catalog:
        # Create catalog DataFrame
        catalog_df = pd.DataFrame(catalog)
        
        # Save catalog
        catalog_file = DATA_DIR / "data_catalog.csv"
        catalog_df.to_csv(catalog_file, index=False)
        
        print(f"   ✅ Catalog created with {len(catalog)} datasets")
        print("\n📊 Data Catalog:")
        print(catalog_df.to_string())
        
        return catalog_df
    else:
        print("   ⚠️ No data files found to catalog")
        return pd.DataFrame()


# In[26]:


def test_setup():
    """
    Test if everything is set up correctly
    """
    print("\n🔧 Testing Setup...")
    print("=" * 60)
    
    # Check Python version
    import sys
    print(f"✅ Python version: {sys.version}")
    
    # Check if directories exist
    if DATA_DIR.exists():
        print(f"✅ Data directory exists: {DATA_DIR}")
    else:
        print(f"❌ Data directory not found: {DATA_DIR}")
    
    if RAW_DATA_DIR.exists():
        print(f"✅ Raw data directory exists: {RAW_DATA_DIR}")
    else:
        print(f"❌ Raw data directory not found: {RAW_DATA_DIR}")
    
    # Check if we can write files
    try:
        test_file = RAW_DATA_DIR / "test.txt"
        test_file.write_text("test")
        test_file.unlink()  # Delete test file
        print("✅ Can write to data directory")
    except Exception as e:
        print(f"❌ Cannot write to data directory: {e}")
    
    # Check internet connection
    try:
        response = requests.get("https://www.google.com", timeout=5)
        print("✅ Internet connection working")
    except:
        print("❌ No internet connection")
    
    print("=" * 60)


# In[28]:


def main():
    """
    Main function to run all data collection
    """
    
    print("=" * 60)
    print("🚀 STARTING DATA COLLECTION PIPELINE")
    print("=" * 60)
    
    # First, test the setup
    test_setup()
    
    # Define World Bank indicators we want
    # These are relevant to infrastructure and climate
    indicators = {
        'EN.GHG.CO2.PC.CE.AR5': 'co2_emissions_per_capita',
        'NY.GDP.PCAP.CD': 'gdp_per_capita',
        'SP.POP.TOTL': 'population_total',
        'EG.FEC.RNEW.ZS': 'renewable_energy_consumption',
        # Note: Some indicators might not have data
        # 'IS.ROD.PAVE.ZS': 'roads_paved_percentage',
        # 'IS.RRS.TOTL.KM': 'railway_lines_total_km'
    }
    
    # Download each indicator
    downloaded_data = {}
    for code, name in indicators.items():
        df = download_world_bank_indicator(code, name)
        if df is not None:
            downloaded_data[name] = df
        
        # Be nice to the API - wait between requests
        time.sleep(1)
    
    # Create sample infrastructure data
    infra_data = download_sample_infrastructure_data()
    
    # Create data catalog
    catalog = create_data_catalog()
    
    print("\n" + "=" * 60)
    print("✅ DATA COLLECTION COMPLETE!")
    print("=" * 60)
    
    if not catalog.empty:
        print(f"\n📁 All data saved to: {RAW_DATA_DIR}")
        print(f"📊 Total datasets collected: {len(catalog)}")
        print(f"💾 Total size: {catalog['size_mb'].sum():.2f} MB")
        
        # Save a summary report
        report = {
            'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'datasets_collected': len(catalog),
            'total_size_mb': float(catalog['size_mb'].sum()),
            'data_directory': str(RAW_DATA_DIR)
        }
        
        report_file = DATA_DIR / "collection_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Collection report saved to: {report_file.name}")
    else:
        print("\n⚠️ No data was collected. Please check the errors above.")
    
    return downloaded_data

if __name__ == "__main__":
    # Run the data collection
    data = main()
    
    print("\n🎉 Script execution completed!")
    print("📚 Next steps:")
    print("   1. Check the data/raw folder for your downloaded files")
    print("   2. Review any error messages above")
    print("   3. Open Jupyter notebook to explore the data")


# In[ ]:




