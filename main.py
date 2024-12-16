import sys
import logging
from datetime import datetime
import sqlite3
import pandas as pd
from data_collection import get_db_connection, create_database, get_run_count

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('covid_flu_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_all_table_counts():
    """Get row counts for all tables"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    counts = {}
    for (table_name,) in tables:
        try:
            result = cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
            counts[table_name] = result[0] if result else 0
        except Exception as e:
            logging.error(f"Error getting count for table {table_name}: {str(e)}")
            counts[table_name] = -1
    
    conn.close()
    return counts

def setup_database():
    """Initialize the database and required tables"""
    try:
        if not create_database():
            logging.error("Failed to create database")
            return False
            
        conn = get_db_connection()
        # Ensure foreign keys are enabled
        conn.execute("PRAGMA foreign_keys = ON;")
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
        success = collect_all_data()
        
        if not success:
            logging.error("Data collection failed")
            return False
        
        # Get and log table counts after collection
        table_counts = get_all_table_counts()
        logging.info("Data collection completed. Current table row counts:")
        for table, count in table_counts.items():
            logging.info(f"- {table}: {count} rows")
            
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

    try:
        # Setup phase
        if not setup_database():
            logging.error("Failed to set up database. Exiting.")
            return

        # Data collection phase
        if not collect_data():
            logging.error("Failed to collect data. Exiting.")
            return

        # Verify database tables
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logging.info(f"Available tables in database: {[table[0] for table in tables]}")
        
        # Get and log current table counts
        table_counts = get_all_table_counts()
        logging.info("Current table row counts:")
        for table, count in table_counts.items():
            logging.info(f"- {table}: {count} rows")
        
        # Get the current run count
        run_count = get_run_count("weather_data")  # Can use any of the data sources
        
        # Only create visualizations if we're on run 5 or later
        if run_count >= 5:
            logging.info("Run 5 or later detected - creating visualizations")
            if not create_visualizations():
                logging.error("Failed to create visualizations. Exiting.")
                return
        else:
            logging.info(f"Run {run_count} - skipping visualizations until run 5")

        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Analysis pipeline completed successfully in {duration}")

    except Exception as e:
        logging.error(f"Unexpected error in main: {str(e)}")
        raise

    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()