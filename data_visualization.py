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
    for start, end, label in seasons:
        color = SEASON_COLORS[label]
        ax.axvspan(pd.to_datetime(start), pd.to_datetime(end), color=color, alpha=0.2, 
                  label=label if label not in seen_labels else "")
        seen_labels.add(label)
    
    ax.set_title(title, fontsize=16)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    handles, labels = ax.get_legend_handles_labels()
    line_handles = [h for h, l in zip(handles, labels) if l == label]
    line_labels = [l for l in labels if l == label]
    season_handles = [h for h, l in zip(handles, labels) if l != label]
    season_labels = [l for l in labels if l != label]
    
    ax.legend(
        line_handles + season_handles,
        line_labels + season_labels,
        loc='upper left',
        bbox_to_anchor=(-0.2, -0.15),
        fontsize=10,
        ncol=2
    )
    
    plt.tight_layout(pad=3)
    plt.show()

def visualize_all_data():
    conn = get_db_connection()
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

    # Michigan COVID Data
    michigan_covid_df = pd.read_sql("SELECT week_id, weekly_cases FROM michigan_covid_data", conn)
    michigan_covid_df["week_id"] = pd.to_datetime(michigan_covid_df["week_id"] + '-0', format='%Y-%W-%w')
    michigan_covid_df = michigan_covid_df[michigan_covid_df["weekly_cases"] > 0]
    
    plot_cases_with_bars(
        michigan_covid_df, "week_id", "weekly_cases", "Michigan COVID Cases", "blue",
        "Michigan COVID-19 Cases, Highlighting Winter and Summer Months",
        "Weekly COVID-19 Cases", start_date, end_date, seasons
    )

    # National COVID Data
    national_covid_df = pd.read_sql("SELECT week_id, weekly_cases FROM national_covid_data", conn)
    national_covid_df["week_id"] = pd.to_datetime(national_covid_df["week_id"] + '-0', format='%Y-%W-%w')
    national_covid_df = national_covid_df[national_covid_df["weekly_cases"] > 0]
    
    plot_cases_with_bars(
        national_covid_df, "week_id", "weekly_cases", "National COVID Cases", "green",
        "National COVID-19 Cases, Highlighting Winter and Summer Months",
        "Weekly COVID-19 Cases", start_date, end_date, seasons
    )

    # Michigan Flu Data
    michigan_flu_df = pd.read_sql(
        "SELECT date, num_ili FROM flu_data_march_2020_to_2023_region_date_ili WHERE region = 'mi'",
        conn
    )
    michigan_flu_df["date"] = pd.to_datetime(michigan_flu_df["date"])
    michigan_flu_df = michigan_flu_df[michigan_flu_df["num_ili"] > 0]
    
    plot_cases_with_bars(
        michigan_flu_df, "date", "num_ili", "Michigan Flu Cases", "red",
        "Michigan Flu Cases, Highlighting Winter and Summer Months",
        "Weekly Flu Cases", start_date, end_date, seasons
    )

    # National Flu Data
    national_flu_df = pd.read_sql(
        "SELECT date, SUM(num_ili) AS total_ili FROM flu_data_march_2020_to_2023_region_date_ili GROUP BY date",
        conn
    )
    national_flu_df["date"] = pd.to_datetime(national_flu_df["date"])
    national_flu_df = national_flu_df[national_flu_df["total_ili"] > 0]
    
    plot_cases_with_bars(
        national_flu_df, "date", "total_ili", "National Flu Cases", "purple",
        "National Flu Cases, Highlighting Winter and Summer Months",
        "Weekly Flu Cases", start_date, end_date, seasons
    )

    conn.close()