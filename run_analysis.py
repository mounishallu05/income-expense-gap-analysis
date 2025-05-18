#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Driver script to run the entire data analysis pipeline:
1. Data acquisition
2. Data processing
3. Data visualization
4. Modeling and prediction
"""

import os
import sys
import logging
import subprocess
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_path, description):
    """Run a Python script and log its output"""
    start_time = time.time()
    logger.info(f"Starting {description}...")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        
        # Log the output
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"[{os.path.basename(script_path)}] {line}")
        
        duration = time.time() - start_time
        logger.info(f"Completed {description} in {duration:.1f} seconds")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {description}")
        logger.error(f"Return code: {e.returncode}")
        
        if e.stdout:
            for line in e.stdout.splitlines():
                logger.info(f"[{os.path.basename(script_path)}] {line}")
        
        if e.stderr:
            for line in e.stderr.splitlines():
                logger.error(f"[{os.path.basename(script_path)}] {line}")
                
        return False

def main():
    """Main function to run the entire pipeline"""
    start_time = time.time()
    logger.info("Starting income vs. expenses analysis pipeline")
    logger.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Define the script paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_acquisition_script = os.path.join(base_dir, 'src', 'data', 'acquire_data.py')
    data_processing_script = os.path.join(base_dir, 'src', 'data', 'process_data.py')
    visualization_script = os.path.join(base_dir, 'src', 'visualization', 'visualize_data.py')
    modeling_script = os.path.join(base_dir, 'src', 'models', 'predict_rent_changes.py')
    
    # Step 1: Data Acquisition
    if not run_script(data_acquisition_script, "data acquisition"):
        logger.error("Data acquisition failed. Pipeline stopped.")
        return False
    
    # Step 2: Data Processing
    if not run_script(data_processing_script, "data processing"):
        logger.error("Data processing failed. Pipeline stopped.")
        return False
    
    # Step 3: Data Visualization
    if not run_script(visualization_script, "data visualization"):
        logger.error("Data visualization failed. Pipeline stopped.")
        return False
    
    # Step 4: Modeling and Prediction
    if not run_script(modeling_script, "modeling and prediction"):
        logger.error("Modeling and prediction failed. Pipeline stopped.")
        return False
    
    total_duration = time.time() - start_time
    logger.info(f"Complete pipeline executed successfully in {total_duration:.1f} seconds")
    logger.info(f"Results are available in the 'results' directory")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 