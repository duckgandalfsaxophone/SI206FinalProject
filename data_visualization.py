import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import sqlite3
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_connection():
    """Get database connection using absolute path"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "final_project.db")
        
        if not os.path.exists(db_path):
            logging.error(f"Database not found at: {db_path}")
            raise FileNotFoundError(f"Database file not found at {db_path}")
            
        logging.info(f"Connecting to database at: {db_path}")
        return sqlite3.connect(db_path)
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        raise

def format_with_commas(x, pos):
    """Format numbers with comma separators"""
    return f"{int(x):,}"

def set_monthly_xticks(ax, start_date, end_date):
    """Set x-axis ticks to show months"""
    months = pd.date_range(start=start_date, end=end_date, freq='MS')
    labels = [date.strftime('%b %Y') if date.month in [3, 6, 9, 12] else '' for date in months]
    ax.set_xticks(months)
    ax.set_xticklabels(labels, rotation=45)
    ax.set_xlabel('Months', fontsize=14)

def plot_cases_with_bars(df, x, y, label, color, title, ylabel, start_date, end_date, seasons):
    """Create plot with cases data and seasonal highlighting"""
    try:
        if df.empty:
            logging.warning(f"No data available for {label}. Skipping plot.")
            return
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot the main data
        ax.plot(df[x], df[y], marker='o', label=label, color=color)
        ax.set_ylabel(ylabel, fontsize=14)
        ax.tick_params(axis='y')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
        
        # Set x-axis ticks and labels
        set_monthly_xticks(ax, start_date, end_date)
        
        # Define season colors
        SEASON_COLORS = {
            "Winter Months": "#003366",
            "Summer Months": "#660000",
        }
        
        # Add seasonal highlighting
        seen_labels = set()
        for start, end, season_label in seasons:
            color = SEASON_COLORS[season_label]
            ax.axvspan(pd.to_datetime(start), pd.to_datetime(end), 
                      color=color, alpha=0.2,
                      label=season_label if season_label not in seen_labels else "")
            seen_labels.add(season_label)
        
        # Set title and grid
        ax.set_title(title, fontsize=16)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        
        # Add legend
        ax.legend(
            loc='upper center', 
            bbox_to_anchor=(0.5, -0.15), 
            fontsize=10, 
            ncol=2
        )
        
        plt.tight_layout(pad=3)
        plt.show()
        
    except Exception as e:
        logging.error(f"Error creating plot for {label}: {str(e)}")
        raise

def process_covid_data(conn, table_name, region=""):
    """Process COVID data from database with proper week_id handling"""
    try:
        query = f"SELECT week_id, weekly_cases FROM {table_name}"
        logging.info(f"Executing query: {query}")
        
        df = pd.read_sql(query, conn)
        if df.empty:
            logging.warning(f"No data found in {table_name}")
            return pd.DataFrame()
            
        # Convert week_id to datetime - handling both string and integer formats
        df["week_id"] = df["week_id"].apply(
            lambda x: pd.to_datetime(
                f'{str(x)[:4]}-W{str(x)[4:].zfill(2)}-1', 
                format='%Y-W%W-%w',
                errors='coerce'
            )
        )
        
        # Filter out zero or negative cases and null dates
        df = df[(df["weekly_cases"] > 0) & (df["week_id"].notna())]
        
        logging.info(f"Processed {len(df)} rows of {region} COVID data")
        return df
        
    except Exception as e:
        logging.error(f"Error processing {region} COVID data: {str(e)}")
        raise

def process_flu_data(conn, region_key):
    """Process flu data from database with proper week_id handling"""
    try:
        query = """
            SELECT week_id, SUM(num_ili) AS total_ili 
            FROM flu_data_march_2020_to_2023 
            WHERE region_key = ? 
            GROUP BY week_id
        """
        logging.info(f"Executing flu data query for region {region_key}")
        
        df = pd.read_sql(query, conn, params=(region_key,))
        if df.empty:
            logging.warning(f"No flu data found for region {region_key}")
            return pd.DataFrame()
            
        # Convert week_id to datetime - handling both string and integer formats
        df["week_id"] = df["week_id"].apply(
            lambda x: pd.to_datetime(
                f'{str(x)[:4]}-W{str(x)[4:].zfill(2)}-1', 
                format='%Y-W%W-%w',
                errors='coerce'
            )
        )
        
        # Filter out zero or negative cases and null dates
        df = df[(df["total_ili"] > 0) & (df["week_id"].notna())]
        
        logging.info(f"Processed {len(df)} rows of flu data for region {region_key}")
        return df
        
    except Exception as e:
        logging.error(f"Error processing flu data for region {region_key}: {str(e)}")
        raise

def validate_database_tables(conn):
    """Validate that all required tables exist in the database"""
    required_tables = [
        'michigan_covid_data',
        'national_covid_data',
        'flu_data_march_2020_to_2023'
    ]
    
    cursor = conn.cursor()
    existing_tables = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    existing_tables = [table[0] for table in existing_tables]
    
    missing_tables = [table for table in required_tables if table not in existing_tables]
    
    if missing_tables:
        raise ValueError(f"Missing required tables: {', '.join(missing_tables)}")
    
    logging.info("All required tables found in database")

def visualize_all_data():
    """Generate all visualizations with error handling"""
    conn = None
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Validate database tables
        validate_database_tables(conn)
        
        # Define date range and seasons
        start_date = "2020-03-01"
        end_date = "2023-03-01"
        
        seasons = [
            ("2020-06-01", "2020-08-31", "Summer Months"),
            ("2020-12-01", "2021-02-28", "Winter Months"),
            ("2021-06-01", "2021-08-31", "Summer Months"),
            ("2021-12-01", "2022-02-28", "Winter Months"),
            ("2022-06-01", "2022-08-31", "Summer Months"),
            ("2022-12-01", "2023-02-28", "Winter Months"),
        ]
        
        # Process and visualize Michigan COVID data
        logging.info("Processing Michigan COVID data")
        michigan_covid_df = process_covid_data(conn, "michigan_covid_data", "Michigan")
        if not michigan_covid_df.empty:
            plot_cases_with_bars(
                michigan_covid_df, "week_id", "weekly_cases",
                "Michigan COVID Cases", "blue",
                "Michigan Weekly New COVID-19 Cases, Highlighting Winter and Summer Months",
                "Weekly New COVID-19 Cases", start_date, end_date, seasons
            )
        
        # Process and visualize National COVID data
        logging.info("Processing National COVID data")
        national_covid_df = process_covid_data(conn, "national_covid_data", "National")
        if not national_covid_df.empty:
            plot_cases_with_bars(
                national_covid_df, "week_id", "weekly_cases",
                "National COVID Cases", "green",
                "National Weekly New COVID-19 Cases, Highlighting Winter and Summer Months",
                "Weekly New COVID-19 Cases", start_date, end_date, seasons
            )
        
        # Process and visualize Michigan Flu data
        logging.info("Processing Michigan Flu data")
        michigan_flu_df = process_flu_data(conn, 1)
        if not michigan_flu_df.empty:
            plot_cases_with_bars(
                michigan_flu_df, "week_id", "total_ili",
                "Michigan Flu Cases", "red",
                "Michigan Weekly New Flu Cases, Highlighting Winter and Summer Months",
                "Weekly New Flu Cases", start_date, end_date, seasons
            )
        
        # Process and visualize National Flu data
        logging.info("Processing National Flu data")
        national_flu_df = process_flu_data(conn, 2)
        if not national_flu_df.empty:
            plot_cases_with_bars(
                national_flu_df, "week_id", "total_ili",
                "National Flu Cases", "purple",
                "National Weekly New Flu Cases, Highlighting Winter and Summer Months",
                "Weekly New Flu Cases", start_date, end_date, seasons
            )
        
        logging.info("All visualizations completed successfully")
        
    except Exception as e:
        logging.error(f"Error in visualization process: {str(e)}")
        raise
        
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    try:
        visualize_all_data()
    except Exception as e:
        logging.error(f"Visualization script failed: {str(e)}")
        sys.exit(1)
