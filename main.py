# -*- coding: utf-8 -*-
"""Main script to coordinate data collection and visualization"""

import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('covid_flu_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def setup_database():
    """Initialize the database and required tables"""
    try:
        from data_collection import get_db_connection
        conn = get_db_connection()
        conn.close()
        logging.info("Database setup successful")
        return True
    except Exception as e:
        logging.error(f"Database setup failed: {str(e)}")
        return False

def collect_data():
    """Collect data from all sources"""
    try:
        from data_collection import collect_all_data
        logging.info("Starting data collection process...")
        collect_all_data()
        logging.info("Data collection completed successfully")
        return True
    except Exception as e:
        logging.error(f"Data collection failed: {str(e)}")
        return False

def create_visualizations():
    """Generate all visualizations"""
    try:
        from data_visualization import visualize_all_data
        logging.info("Starting visualization generation...")
        visualize_all_data()
        logging.info("Visualizations created successfully")
        return True
    except Exception as e:
        logging.error(f"Visualization creation failed: {str(e)}")
        return False

def main():
    """Main execution function"""
    start_time = datetime.now()
    logging.info("Starting COVID and Flu analysis pipeline")
    
    # Setup phase
    if not setup_database():
        logging.error("Failed to set up database. Exiting.")
        sys.exit(1)
    
    # Data collection phase
    if not collect_data():
        logging.error("Failed to collect data. Exiting.")
        sys.exit(1)
    
    # Visualization phase
    if not create_visualizations():
        logging.error("Failed to create visualizations. Exiting.")
        sys.exit(1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Analysis pipeline completed successfully in {duration}")

if __name__ == "__main__":
    main()