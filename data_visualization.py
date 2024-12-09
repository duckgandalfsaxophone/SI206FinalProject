import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import sqlite3

def get_db_connection():
    return sqlite3.connect("final_project.db")

def format_with_commas(x, pos):
    return f"{int(x):,}"

def set_monthly_xticks(ax, start_date, end_date):
    months = pd.date_range(start=start_date, end=end_date, freq='MS')
    labels = [date.strftime('%b %Y') if date.month in [3, 6, 9, 12] else '' for date in months]
    ax.set_xticks(months)
    ax.set_xticklabels(labels, rotation=45)
    ax.set_xlabel('Months', fontsize=14)

def plot_data(df, x_column, y_column, title, ylabel, label, color, start_date, end_date):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df[x_column], df[y_column], marker='o', label=label, color=color)
    ax.set_title(title, fontsize=16)
    ax.set_ylabel(ylabel, fontsize=14)
    ax.yaxis.set_major_formatter(FuncFormatter(format_with_commas))
    set_monthly_xticks(ax, start_date, end_date)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.legend()
    plt.tight_layout()
    plt.show()

def plot_cases_with_bars(df, x, y, label, color, title, ylabel, start_date, end_date, seasons):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    if df.empty:
        print(f"No data available for {label}. Skipping plot.")
        return
    
    ax.plot(df[x], df[y], marker='o', label=label, color=color)
    ax.set_ylabel(ylabel, fontsize=14)
    ax.tick_params(axis='y')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    
    set_monthly_xticks(ax, start_date, end_date)
    
    SEASON_COLORS = {
        "Winter Months": "#003366",
        "Summer Months": "#660000",
    }
    
    seen_labels = set()
    for start, end, season_label in seasons:
        color = SEASON_COLORS[season_label]
        ax.axvspan(pd.to_datetime(start), pd.to_datetime(end), color=color, alpha=0.2, 
                  label=season_label if season_label not in seen_labels else "")
        seen_labels.add(season_label)
    
    ax.set_title(title, fontsize=16)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    ax.legend(
        loc='upper center', bbox_to_anchor=(0.5, -0.15), fontsize=10, ncol=2
    )
    plt.tight_layout(pad=3)
    plt.show()


def visualize_all_data():
    """
    Visualize all datasets with seasonal bars and proper handling of errors and data validation.
    """
    conn = get_db_connection()
    start_date = "2020-03-01"
    end_date = "2023-03-01"

    # Define seasonal periods
    seasons = [
        ("2020-06-01", "2020-08-31", "Summer Months"),
        ("2020-12-01", "2021-02-28", "Winter Months"),
        ("2021-06-01", "2021-08-31", "Summer Months"),
        ("2021-12-01", "2022-02-28", "Winter Months"),
        ("2022-06-01", "2022-08-31", "Summer Months"),
        ("2022-12-01", "2023-02-28", "Winter Months"),
    ]

    try:
        # Michigan COVID Data
        michigan_covid_df = pd.read_sql("SELECT week_id, weekly_cases FROM michigan_covid_data", conn)

        # Fix: Ensure `week_id` is processed safely with error handling
        michigan_covid_df["week_id"] = michigan_covid_df["week_id"].astype(str).apply(
            lambda x: pd.to_datetime(f'{x[:4]}-W{x[4:]}-1', format='%G-W%V-%u', errors='coerce')
        )
        michigan_covid_df = michigan_covid_df[michigan_covid_df["weekly_cases"] > 0]

        if not michigan_covid_df.empty:
            plot_cases_with_bars(
                michigan_covid_df, "week_id", "weekly_cases", "Michigan COVID Cases", "blue",
                "Michigan Weekly New COVID-19 Cases, Highlighting Winter and Summer Months",
                "Weekly COVID-19 Cases", start_date, end_date, seasons
            )
        else:
            print("No Michigan COVID data available to plot.")

    except Exception as e:
        print(f"Error processing Michigan COVID data: {e}")

    try:
        # National COVID Data
        national_covid_df = pd.read_sql("SELECT week_id, weekly_cases FROM national_covid_data", conn)

        # Fix: Ensure `week_id` is processed safely with error handling
        national_covid_df["week_id"] = national_covid_df["week_id"].astype(str).apply(
            lambda x: pd.to_datetime(f'{x[:4]}-W{x[4:]}-1', format='%G-W%V-%u', errors='coerce')
        )
        national_covid_df = national_covid_df[national_covid_df["weekly_cases"] > 0]

        if not national_covid_df.empty:
            plot_cases_with_bars(
                national_covid_df, "week_id", "weekly_cases", "National COVID Cases", "green",
                "National Weekly New COVID-19 Cases, Highlighting Winter and Summer Months",
                "Weekly COVID-19 Cases", start_date, end_date, seasons
            )
        else:
            print("No National COVID data available to plot.")

    except Exception as e:
        print(f"Error processing National COVID data: {e}")

    try:
        # Michigan Flu Data
        michigan_flu_df = pd.read_sql(
            "SELECT date, num_ili FROM flu_data_march_2020_to_2023_region_date_ili WHERE region = 'mi'",
            conn
        )

        # Fix: Validate `date` column and filter data
        michigan_flu_df["date"] = pd.to_datetime(michigan_flu_df["date"], errors='coerce')
        michigan_flu_df = michigan_flu_df[michigan_flu_df["num_ili"] > 0]

        if not michigan_flu_df.empty:
            plot_cases_with_bars(
                michigan_flu_df, "date", "num_ili", "Michigan Flu Cases", "red",
                "Michigan Weekly New Flu Cases, Highlighting Winter and Summer Months",
                "Weekly Flu Cases", start_date, end_date, seasons
            )
        else:
            print("No Michigan Flu data available to plot.")

    except Exception as e:
        print(f"Error processing Michigan Flu data: {e}")

    try:
        # National Flu Data
        national_flu_df = pd.read_sql(
            "SELECT date, SUM(num_ili) AS total_ili FROM flu_data_march_2020_to_2023_region_date_ili GROUP BY date",
            conn
        )

        # Fix: Validate `date` column and filter data
        national_flu_df["date"] = pd.to_datetime(national_flu_df["date"], errors='coerce')
        national_flu_df = national_flu_df[national_flu_df["total_ili"] > 0]

        if not national_flu_df.empty:
            plot_cases_with_bars(
                national_flu_df, "date", "total_ili", "National Flu Cases", "purple",
                "National Flu Cases, Highlighting Winter and Summer Months",
                "Weekly Flu Cases", start_date, end_date, seasons
            )
        else:
            print("No National Flu data available to plot.")

    except Exception as e:
        print(f"Error processing National Flu data: {e}")

    finally:
        conn.close()
