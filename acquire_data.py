#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script handles data acquisition from multiple sources:
1. BLS Consumer Expenditure Survey
2. US Census American Community Survey
3. HUD Fair Market Rent data
4. USPS Change of Address data
"""

import os
import requests
import pandas as pd
from census import Census
from us import states
import json
import logging
from datetime import datetime
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create data directories if they don't exist
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw')
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def get_bls_expenditure_data(api_key=None):
    """
    Fetch Consumer Expenditure Survey data from the Bureau of Labor Statistics API
    """
    logger.info("Fetching BLS Consumer Expenditure Survey data")
    
    if not api_key:
        logger.warning("No BLS API key provided. Using public access (limited requests)")
    
    # BLS API endpoint
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    
    # Consumer expenditure series codes (examples)
    # CXUT0100AA - All consumer units: Total average annual expenditures
    # CXUT0200AA - All consumer units: Food average annual expenditures
    # CXUT0400AA - All consumer units: Housing average annual expenditures
    
    series_ids = [
        'CXUT0100AA',  # Total expenditures
        'CXUT0200AA',  # Food
        'CXUT0400AA',  # Housing
        'CXUT0450AA',  # Shelter
        'CXUT0500AA',  # Transportation
        'CXUT0600AA',  # Healthcare
    ]
    
    # Request parameters
    data = {
        "seriesid": series_ids,
        "startyear": "2010",  
        "endyear": "2023",
        "registrationkey": api_key
    }
    
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        result = response.json()
        
        if result['status'] == 'REQUEST_SUCCEEDED':
            # Convert to DataFrame
            all_data = []
            for series_data in result['Results']['series']:
                series_id = series_data['seriesID']
                
                # Get series name based on ID
                if series_id == 'CXUT0100AA':
                    category = 'Total Expenditures'
                elif series_id == 'CXUT0200AA':
                    category = 'Food'
                elif series_id == 'CXUT0400AA':
                    category = 'Housing'
                elif series_id == 'CXUT0450AA':
                    category = 'Shelter'
                elif series_id == 'CXUT0500AA':
                    category = 'Transportation'
                elif series_id == 'CXUT0600AA':
                    category = 'Healthcare'
                else:
                    category = series_id
                    
                for item in series_data['data']:
                    year = item['year']
                    value = float(item['value'])
                    all_data.append({
                        'category': category,
                        'year': year, 
                        'value': value
                    })
            
            df = pd.DataFrame(all_data)
            
            # Save to file
            output_file = os.path.join(RAW_DATA_DIR, 'bls_consumer_expenditure.csv')
            df.to_csv(output_file, index=False)
            logger.info(f"BLS data saved to {output_file}")
            
            return df
            
        else:
            logger.error(f"BLS API request failed: {result['message']}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching BLS data: {e}")
        return None

def get_census_data(api_key=None):
    """
    Fetch income and population data from the Census American Community Survey API
    """
    logger.info("Fetching Census American Community Survey data")
    
    if not api_key:
        logger.warning("No Census API key provided. Skipping Census data acquisition.")
        return None
    
    try:
        c = Census(api_key)
        
        # Get data for all states
        # B19013_001E = Median household income
        # B01003_001E = Total population
        # B25064_001E = Median gross rent
        
        state_data = c.acs5.get(
            ('NAME', 'B19013_001E', 'B01003_001E', 'B25064_001E'),
            {'for': 'state:*'},
            year=2022
        )
        
        # Get data for metropolitan areas
        metro_data = c.acs5.get(
            ('NAME', 'B19013_001E', 'B01003_001E', 'B25064_001E'),
            {'for': 'metropolitan statistical area/micropolitan statistical area:*'},
            year=2022
        )
        
        # Create DataFrames
        df_states = pd.DataFrame(state_data)
        df_states = df_states.rename(columns={
            'B19013_001E': 'median_household_income',
            'B01003_001E': 'total_population',
            'B25064_001E': 'median_gross_rent',
            'NAME': 'state_name'
        })
        
        df_metros = pd.DataFrame(metro_data)
        df_metros = df_metros.rename(columns={
            'B19013_001E': 'median_household_income',
            'B01003_001E': 'total_population',
            'B25064_001E': 'median_gross_rent',
            'NAME': 'metro_name'
        })
        
        # Save to files
        states_file = os.path.join(RAW_DATA_DIR, 'census_states_acs.csv')
        metros_file = os.path.join(RAW_DATA_DIR, 'census_metros_acs.csv')
        
        df_states.to_csv(states_file, index=False)
        df_metros.to_csv(metros_file, index=False)
        
        logger.info(f"Census state data saved to {states_file}")
        logger.info(f"Census metro data saved to {metros_file}")
        
        return df_states, df_metros
        
    except Exception as e:
        logger.error(f"Error fetching Census data: {e}")
        return None

def get_hud_rent_data():
    """
    Fetch Fair Market Rent data from HUD
    """
    logger.info("Fetching HUD Fair Market Rent data")
    
    # HUD Fair Market Rent (FMR) data URL
    # Using the most recent data available via their API endpoint
    url = "https://www.huduser.gov/portal/datasets/fmr/fmr2022/FY22_4050_FMRs.zip"
    
    try:
        # Create a temporary file to store the ZIP
        import tempfile
        from zipfile import ZipFile
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            # Download the zip file
            response = requests.get(url)
            response.raise_for_status()
            tmp_file.write(response.content)
            tmp_file_path = tmp_file.name
        
        # Extract the zip file
        with ZipFile(tmp_file_path, 'r') as zip_ref:
            # List all files in the archive
            file_list = zip_ref.namelist()
            csv_files = [f for f in file_list if f.endswith('.csv')]
            
            if csv_files:
                # Extract the first CSV file found
                with zip_ref.open(csv_files[0]) as file:
                    df = pd.read_csv(file)
                    
                # Save to file
                output_file = os.path.join(RAW_DATA_DIR, 'hud_fair_market_rent.csv')
                df.to_csv(output_file, index=False)
                logger.info(f"HUD FMR data saved to {output_file}")
                
                # Clean up the temporary file
                os.unlink(tmp_file_path)
                
                return df
            else:
                logger.error("No CSV files found in the HUD ZIP archive")
                return None
                
    except Exception as e:
        logger.error(f"Error fetching HUD data: {e}")
        return None

def get_migration_data():
    """
    Simulate migration data (as actual USPS Change of Address data requires payment)
    This will create synthetic data based on publicly available migration trend information
    """
    logger.info("Creating synthetic migration data (USPS change of address data requires payment)")
    
    # List of major metro areas people are leaving
    outflow_metros = [
        "New York-Newark-Jersey City, NY-NJ-PA",
        "Los Angeles-Long Beach-Anaheim, CA",
        "Chicago-Naperville-Elgin, IL-IN-WI",
        "San Francisco-Oakland-Berkeley, CA",
        "Boston-Cambridge-Newton, MA-NH"
    ]
    
    # List of metro areas people are moving to
    inflow_metros = [
        "Austin-Round Rock-Georgetown, TX",
        "Phoenix-Mesa-Chandler, AZ",
        "Nashville-Davidson--Murfreesboro--Franklin, TN",
        "Raleigh-Cary, NC",
        "Tampa-St. Petersburg-Clearwater, FL",
        "Dallas-Fort Worth-Arlington, TX",
        "Charlotte-Concord-Gastonia, NC-SC",
        "Jacksonville, FL",
        "Salt Lake City, UT",
        "Denver-Aurora-Lakewood, CO"
    ]
    
    # Generate synthetic data
    import numpy as np
    np.random.seed(42)  # For reproducibility
    
    data = []
    years = range(2018, 2023)
    
    for year in years:
        # Increasing outflow from major metros over time
        outflow_factor = 1 + 0.15 * (year - 2018)
        
        for origin in outflow_metros:
            # Base outflow population (higher for larger metros)
            if "New York" in origin:
                base = 120000
            elif "Los Angeles" in origin:
                base = 100000
            elif "Chicago" in origin:
                base = 80000
            elif "San Francisco" in origin:
                base = 70000
            else:
                base = 50000
                
            # Add variation and trend
            outflow = int(base * outflow_factor * np.random.normal(1, 0.1))
            
            # Distribute outflow to destination metros
            for dest in inflow_metros:
                # Different destinations get different proportions
                if dest == "Austin-Round Rock-Georgetown, TX":
                    prop = 0.18
                elif dest in ["Phoenix-Mesa-Chandler, AZ", "Tampa-St. Petersburg-Clearwater, FL"]:
                    prop = 0.15
                elif dest in ["Raleigh-Cary, NC", "Dallas-Fort Worth-Arlington, TX"]:
                    prop = 0.12
                else:
                    prop = 0.08
                    
                # Add some noise to the proportion
                prop = prop * np.random.normal(1, 0.2)
                
                # Calculate migrants and add to data
                migrants = max(int(outflow * prop), 0)
                data.append({
                    'year': year,
                    'origin_metro': origin,
                    'destination_metro': dest,
                    'num_migrants': migrants
                })
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Save to file
    output_file = os.path.join(RAW_DATA_DIR, 'synthetic_migration_data.csv')
    df.to_csv(output_file, index=False)
    logger.info(f"Synthetic migration data saved to {output_file}")
    
    return df

def main():
    """
    Main function to acquire all datasets
    """
    logger.info("Starting data acquisition process")
    
    # Get BLS Consumer Expenditure data
    # Replace None with your API key if you have one
    bls_data = get_bls_expenditure_data(api_key=None)
    
    # Get Census income and population data
    # Replace None with your API key if you have one
    census_data = get_census_data(api_key=None)
    
    # Get HUD Fair Market Rent data
    hud_data = get_hud_rent_data()
    
    # Get migration data (synthetic)
    migration_data = get_migration_data()
    
    logger.info("Data acquisition process completed")

if __name__ == "__main__":
    main() 