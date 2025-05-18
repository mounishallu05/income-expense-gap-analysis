#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script processes and combines data from multiple sources:
1. BLS Consumer Expenditure Survey
2. US Census American Community Survey
3. HUD Fair Market Rent data
4. Migration data
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Data directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')

# Create directories if they don't exist
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def process_bls_expenditure_data():
    """
    Process the BLS Consumer Expenditure Survey data
    """
    logger.info("Processing BLS Consumer Expenditure data")
    
    file_path = os.path.join(RAW_DATA_DIR, 'bls_consumer_expenditure.csv')
    
    try:
        # Read the raw data
        df = pd.read_csv(file_path)
        
        # Convert year to integer
        df['year'] = df['year'].astype(int)
        
        # Pivot the data to have categories as columns
        pivot_df = df.pivot(index='year', columns='category', values='value')
        
        # Calculate ratio of housing to income
        # We need to merge with income data later
        
        # Reset the index to make year a column again
        pivot_df.reset_index(inplace=True)
        
        # Calculate annual percentage changes
        for category in pivot_df.columns:
            if category != 'year':
                pivot_df[f'{category}_pct_change'] = pivot_df[category].pct_change() * 100
        
        # Save to processed data directory
        output_file = os.path.join(PROCESSED_DATA_DIR, 'processed_bls_expenditure.csv')
        pivot_df.to_csv(output_file, index=False)
        logger.info(f"Processed BLS data saved to {output_file}")
        
        return pivot_df
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error processing BLS data: {e}")
        return None

def process_census_data():
    """
    Process the Census ACS data for states and metros
    """
    logger.info("Processing Census ACS data")
    
    states_file = os.path.join(RAW_DATA_DIR, 'census_states_acs.csv')
    metros_file = os.path.join(RAW_DATA_DIR, 'census_metros_acs.csv')
    
    try:
        # Process state data
        if os.path.exists(states_file):
            df_states = pd.read_csv(states_file)
            
            # Convert columns to appropriate types
            df_states['median_household_income'] = pd.to_numeric(df_states['median_household_income'], errors='coerce')
            df_states['total_population'] = pd.to_numeric(df_states['total_population'], errors='coerce')
            df_states['median_gross_rent'] = pd.to_numeric(df_states['median_gross_rent'], errors='coerce')
            
            # Calculate rent to income ratio
            df_states['rent_to_income_ratio'] = (df_states['median_gross_rent'] * 12) / df_states['median_household_income']
            
            # Save processed state data
            states_output = os.path.join(PROCESSED_DATA_DIR, 'processed_states_acs.csv')
            df_states.to_csv(states_output, index=False)
            logger.info(f"Processed state data saved to {states_output}")
        else:
            logger.warning(f"State data file not found: {states_file}")
            df_states = None
        
        # Process metro data
        if os.path.exists(metros_file):
            df_metros = pd.read_csv(metros_file)
            
            # Convert columns to appropriate types
            df_metros['median_household_income'] = pd.to_numeric(df_metros['median_household_income'], errors='coerce')
            df_metros['total_population'] = pd.to_numeric(df_metros['total_population'], errors='coerce')
            df_metros['median_gross_rent'] = pd.to_numeric(df_metros['median_gross_rent'], errors='coerce')
            
            # Calculate rent to income ratio
            df_metros['rent_to_income_ratio'] = (df_metros['median_gross_rent'] * 12) / df_metros['median_household_income']
            
            # Extract state information from metro name
            df_metros['state'] = df_metros['metro_name'].str.extract(r', ([A-Z]{2}(?:-[A-Z]{2})*)$')
            
            # Save processed metro data
            metros_output = os.path.join(PROCESSED_DATA_DIR, 'processed_metros_acs.csv')
            df_metros.to_csv(metros_output, index=False)
            logger.info(f"Processed metro data saved to {metros_output}")
        else:
            logger.warning(f"Metro data file not found: {metros_file}")
            df_metros = None
            
        return df_states, df_metros
        
    except Exception as e:
        logger.error(f"Error processing Census data: {e}")
        return None, None

def process_hud_rent_data():
    """
    Process HUD Fair Market Rent data
    """
    logger.info("Processing HUD Fair Market Rent data")
    
    file_path = os.path.join(RAW_DATA_DIR, 'hud_fair_market_rent.csv')
    
    try:
        if os.path.exists(file_path):
            # Read the raw data
            df = pd.read_csv(file_path)
            
            # Keep only essential columns and rename them
            # The actual column names will depend on the structure of the downloaded file
            # This is a placeholder that needs to be adjusted based on actual data
            
            # Assuming columns like: area_name, state, county_code, fmr_0, fmr_1, fmr_2, fmr_3, fmr_4
            # Where fmr_N represents the Fair Market Rent for N-bedroom units
            
            try:
                # Select and rename columns - adjust based on actual column names
                cols_to_keep = ['area_name', 'state', 'county_code', 
                               'fmr_0', 'fmr_1', 'fmr_2', 'fmr_3', 'fmr_4']
                
                # Only keep columns that actually exist in the DataFrame
                cols_to_keep = [col for col in cols_to_keep if col in df.columns]
                
                processed_df = df[cols_to_keep]
                
                # Calculate average rent across bedroom sizes
                rent_cols = [col for col in processed_df.columns if col.startswith('fmr_')]
                if rent_cols:
                    processed_df['avg_rent'] = processed_df[rent_cols].mean(axis=1)
                
                # Standardize area names to match with Census data if possible
                
                # Save to processed data directory
                output_file = os.path.join(PROCESSED_DATA_DIR, 'processed_hud_rent.csv')
                processed_df.to_csv(output_file, index=False)
                logger.info(f"Processed HUD rent data saved to {output_file}")
                
                return processed_df
                
            except Exception as e:
                logger.error(f"Error processing HUD columns: {e}")
                
                # Fallback: save the entire DataFrame with original column names
                output_file = os.path.join(PROCESSED_DATA_DIR, 'processed_hud_rent.csv')
                df.to_csv(output_file, index=False)
                logger.info(f"Saved original HUD data to {output_file}")
                
                return df
        else:
            logger.warning(f"HUD rent data file not found: {file_path}")
            return None
            
    except Exception as e:
        logger.error(f"Error processing HUD data: {e}")
        return None

def process_migration_data():
    """
    Process migration data
    """
    logger.info("Processing migration data")
    
    file_path = os.path.join(RAW_DATA_DIR, 'synthetic_migration_data.csv')
    
    try:
        if os.path.exists(file_path):
            # Read the raw data
            df = pd.read_csv(file_path)
            
            # Group by destination and year
            inflow_by_dest = df.groupby(['destination_metro', 'year'])['num_migrants'].sum().reset_index()
            inflow_by_dest.rename(columns={'num_migrants': 'total_inflow'}, inplace=True)
            
            # Group by origin and year
            outflow_by_origin = df.groupby(['origin_metro', 'year'])['num_migrants'].sum().reset_index()
            outflow_by_origin.rename(columns={'num_migrants': 'total_outflow'}, inplace=True)
            
            # Calculate net migration for each metro area
            # We need to merge inflow and outflow data

            # Destinations as columns
            dest_df = df.pivot_table(
                index=['origin_metro', 'year'],
                columns='destination_metro',
                values='num_migrants',
                aggfunc='sum',
                fill_value=0
            ).reset_index()
            
            # Save processed data
            inflow_file = os.path.join(PROCESSED_DATA_DIR, 'processed_migration_inflow.csv')
            outflow_file = os.path.join(PROCESSED_DATA_DIR, 'processed_migration_outflow.csv')
            dest_file = os.path.join(PROCESSED_DATA_DIR, 'processed_migration_destinations.csv')
            
            inflow_by_dest.to_csv(inflow_file, index=False)
            outflow_by_origin.to_csv(outflow_file, index=False)
            dest_df.to_csv(dest_file)
            
            logger.info(f"Processed migration data saved to {inflow_file}, {outflow_file}, and {dest_file}")
            
            return inflow_by_dest, outflow_by_origin, dest_df
        else:
            logger.warning(f"Migration data file not found: {file_path}")
            return None, None, None
            
    except Exception as e:
        logger.error(f"Error processing migration data: {e}")
        return None, None, None

def combine_data_for_analysis():
    """
    Combine data from different sources for analysis
    """
    logger.info("Combining data for analysis")
    
    try:
        # Load processed data
        bls_file = os.path.join(PROCESSED_DATA_DIR, 'processed_bls_expenditure.csv')
        metros_file = os.path.join(PROCESSED_DATA_DIR, 'processed_metros_acs.csv')
        inflow_file = os.path.join(PROCESSED_DATA_DIR, 'processed_migration_inflow.csv')
        
        # Check if all required files exist
        if not (os.path.exists(bls_file) and os.path.exists(metros_file) and os.path.exists(inflow_file)):
            logger.error("Some required processed data files are missing")
            return None
        
        # Load the data
        bls_data = pd.read_csv(bls_file)
        metros_data = pd.read_csv(metros_file)
        inflow_data = pd.read_csv(inflow_file)
        
        # Add year information to metros data (use most recent year from BLS data)
        metros_data['year'] = bls_data['year'].max()
        
        # Merge metro data with migration inflow data
        # We need to standardize metro area names first
        
        # For metro names in inflow data
        inflow_data['destination_metro_clean'] = inflow_data['destination_metro'].str.replace(r', [A-Z]{2}(?:-[A-Z]{2})*$', '', regex=True)
        
        # For metro names in Census data
        metros_data['metro_name_clean'] = metros_data['metro_name'].str.replace(r', [A-Z]{2}(?:-[A-Z]{2})*$', '', regex=True)
        
        # Create a merged dataset
        # This is a simplified merge, actual implementation might need more complex matching
        merged_data = pd.merge(
            metros_data,
            inflow_data,
            left_on=['metro_name_clean', 'year'],
            right_on=['destination_metro_clean', 'year'],
            how='left'
        )
        
        # Fill NaN values for migration data (where we don't have migration information)
        merged_data['total_inflow'].fillna(0, inplace=True)
        
        # Save the combined dataset
        output_file = os.path.join(PROCESSED_DATA_DIR, 'combined_analysis_data.csv')
        merged_data.to_csv(output_file, index=False)
        logger.info(f"Combined data for analysis saved to {output_file}")
        
        return merged_data
        
    except Exception as e:
        logger.error(f"Error combining data: {e}")
        return None

def main():
    """
    Main function to process all datasets
    """
    logger.info("Starting data processing")
    
    # Process individual datasets
    bls_data = process_bls_expenditure_data()
    states_data, metros_data = process_census_data()
    hud_data = process_hud_rent_data()
    inflow_data, outflow_data, dest_data = process_migration_data()
    
    # Combine data for analysis
    combined_data = combine_data_for_analysis()
    
    logger.info("Data processing completed")

if __name__ == "__main__":
    main() 