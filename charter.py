import logging
import os

from dotenv import load_dotenv
from logger_setup import setup_logging

import pandas as pd
import matplotlib.pyplot as plt
import glob

load_dotenv()
logger = setup_logging(level=logging.INFO)

MONGO_RESULT_DIR = os.environ.get("MONGO_RESULT_DIR")
MONGO_FILENAME = os.environ.get("MONGO_FILENAME")
MOBILITY_RESULT_DIR = os.environ.get("MOBILITY_RESULT_DIR")
MOBILITY_FILENAME = os.environ.get("MOBILITY_FILENAME")
INFLUX_RESULT_DIR = os.environ.get("INFLUX_RESULT_DIR")
INFLUX_FILENAME = os.environ.get("INFLUX_FILENAME")
SPATIAL_FILENAME = os.environ.get("SPATIAL_FILENAME")
TEMPORAL_FILENAME = os.environ.get("TEMPORAL_FILENAME")
SPATIOTEMPORAL_FILENAME = os.environ.get("SPATIOTEMPORAL_FILENAME")
DELIMITER_FILENAME = os.environ.get("DELIMITER_FILENAME")
SUFFIX_FILENAME = os.environ.get("SUFFIX_FILENAME")
WILDCARD = os.environ.get("WILDCARD")

# Plots results for one database. Files should ideally be something like <path_to_results>/mongo_spatial*.csv or <path>/influx_temporal*.csv
def load_combine_and_plot_results(files: str|list):
    if isinstance(files, str):
        if '*' in files:
            files = glob.glob(files)
        else:
            files = [files]

    combined_df = pd.DataFrame()
    for file in files:
        try:
            df = pd.read_csv(file)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except Exception as e:
            logger.info(f"Could not process file {file}. Error: {e}")

    if combined_df.empty:
        logger.error("No data to process. Exiting.")
        return

    if 'Id' in combined_df.columns:
        combined_df.drop(columns=['Id'], inplace=True)

    numeric_cols = combined_df.select_dtypes(include=['number']).columns
    combined_df[numeric_cols] = combined_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    has_bbox = 'Bbox id' in combined_df.columns and combined_df['Bbox id'].notna().all()
    has_timespan = 'Timespan id' in combined_df.columns and combined_df['Timespan id'].notna().all()

    non_zero_results_df = combined_df[combined_df['No results'] > 0].copy()
    non_zero_results_df['Query time per result'] = non_zero_results_df['Query time'] / non_zero_results_df['No results']

    # For later. Will be using these in conclusions.
    avg_query_time_per_result = non_zero_results_df['Query time per result'].mean()
    avg_query_time = combined_df['Query time'].mean()

    plt.figure(figsize=(10, 6))

    if has_bbox:
        avg_query_time_per_bbox = combined_df.groupby('Bbox id')['Query time'].mean()
        avg_query_time_per_bbox.plot(kind='bar', figsize=(12, 6), title="Average Query Time per Bbox", color='orange')
        plt.ylabel("Average Query Time (s)")
        plt.grid(False)
        plt.show()

    if has_timespan:
        avg_query_time_per_timespan = combined_df.groupby('Timespan id')['Query time'].mean()
        avg_query_time_per_timespan.plot(kind='bar', figsize=(12, 6), title="Average Query Time per Timespan",
                                         color='purple')
        plt.ylabel("Average Query Time (s)")
        plt.grid(False)
        plt.show()

def load_files(file_patterns: dict):
    db_dataframes = {}
    for db_name, patterns in file_patterns.items():
        combined_df = pd.DataFrame()
        for pattern in patterns:
            files = glob.glob(pattern) if isinstance(pattern, str) else pattern
            for file in files:
                try:
                    df = pd.read_csv(file)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                except Exception as e:
                    logger.error(f"Could not process file {file}. Error: {e}")
        if 'Id' in combined_df.columns:
            combined_df.drop(columns=['Id'], inplace=True)
        db_dataframes[db_name] = combined_df
    return db_dataframes

def plot_comparison(db_dataframes, column_to_group, title, ylabel, save_location="/"):
    plt.figure(figsize=(12, 6))
    for db_name, df in db_dataframes.items():
        if column_to_group in df.columns and df[column_to_group].notna().any():
            grouped_avg = df.groupby(column_to_group)['Query time'].mean()
            grouped_avg.plot(label=db_name, marker='o', linestyle='--')
        else:
            logger.info(f"Column {column_to_group} is not available or empty for {db_name}.")

    plt.title(title)
    plt.xlabel(column_to_group)
    plt.ylabel(ylabel)
    plt.legend()
    plt.grid(True)
    plt.savefig(f"{save_location}/{title.lower().replace(' ', '_').replace(':', '')}.png")
    plt.show()

def plot_comparison_bar(db_dataframes, column_to_group, title, ylabel, plot_values: bool = False, save_location="/"):
    comparison_data = {}
    for db_name, df in db_dataframes.items():
        if column_to_group in df.columns and df[column_to_group].notna().any():
            grouped_avg = df.groupby(column_to_group)['Query time'].mean()
            comparison_data[db_name] = grouped_avg
        else:
            logger.info(f"Column {column_to_group} is not available or empty for {db_name}.")
            # comparison_data[db_name] = pd.Series(dtype=float)

    comparison_df = pd.DataFrame(comparison_data).fillna(0)

    ax = comparison_df.plot(kind='bar', figsize=(12, 6), width=0.8)
    plt.title(title)
    plt.xlabel(column_to_group)
    plt.ylabel(ylabel)
    plt.legend(title="Database")
    plt.grid(axis='y')
    plt.xticks(rotation=45)

    if plot_values:
        for container in ax.containers:
            for bar in container:
                height = bar.get_height()
                if height > 0:
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,  # X
                        height,  # Y
                        f"{height:.2f}",  # Text: Rounded to 2 decimals
                        ha='center',  # Horizontal alignment
                        va='bottom',  # Vertical alignment
                        rotation=90,
                        fontsize=10
                    )
    plt.savefig(f"{save_location}/{title.lower().replace(' ', '_').replace(':', '')}{('_annotated' if plot_values else '')}.png")
    plt.show()
'''
file patterns should look like this:
file_patterns = {
    "influxdb": [influx_temporal_results],
    "mongodb": [mongo_spatiotemporal_results],
    "mobilitydb": [mobility_spatiotemporal_results]
}
where results are in the same form as files in "load_combine_and_plot_results"
'''
def compare_databases(file_patterns, save_location="/"):
    db_dataframes = load_files(file_patterns)

    plot_comparison(
        db_dataframes,
        column_to_group='Bbox id',
        title="Average Time for Each Bbox: MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        save_location=save_location
    )

    plot_comparison(
        db_dataframes,
        column_to_group='Timespan id',
        title="Average Time for Each Timespan: InfluxDB vs MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        save_location=save_location
    )

    plot_comparison_bar(
        db_dataframes,
        column_to_group='Bbox id',
        title="Average Time for Each Bbox: MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        plot_values = True,
        save_location=save_location
    )

    plot_comparison_bar(
        db_dataframes,
        column_to_group='Bbox id',
        title="Average Time for Each Bbox: MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        save_location=save_location
    )

    plot_comparison_bar(
        db_dataframes,
        column_to_group='Timespan id',
        title="Average Time for Each Timespan: InfluxDB vs MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        plot_values=True,
        save_location=save_location
    )

    plot_comparison_bar(
        db_dataframes,
        column_to_group='Timespan id',
        title="Average Time for Each Timespan: InfluxDB vs MobilityDB vs MongoDB",
        ylabel="Average Query Time (s)",
        save_location=save_location
    )

'''
These compare_bboxes or compare_timespans should only get db_dataframes loaded by "load_files", but only with files containing either only timespans or only bboxes.
'''
def compare_bboxes_no_results(file_patterns):
    db_dataframes = load_files(file_patterns)

    for db_name, df in db_dataframes.items():
        plt.figure(figsize=(12, 6))
        if 'Timespan id' not in df.columns or df['Timespan id'].isna().all():
            if 'Bbox id' in df.columns and df['Bbox id'].notna().any():
                grouped_results = df.groupby('Bbox id')['No results'].sum()
                grouped_results.plot(kind='bar', color='blue', alpha=0.7)
                plt.title(f"Number of Results per Bbox for {db_name}")
                plt.xlabel("Bbox id")
                plt.ylabel("Number of Results")
                plt.grid(axis='y')
            else:
                logger.error(f"No valid 'Bbox id' column in {db_name} for plotting.")
        plt.show()

def compare_timespans_no_results(file_patterns):
    db_dataframes = load_files(file_patterns)
    for db_name, df in db_dataframes.items():
        plt.figure(figsize=(12, 6))
        if 'Bbox id' not in df.columns or df['Bbox id'].isna().all():
            if 'Timespan id' in df.columns and df['Timespan id'].notna().any():
                grouped_results = df.groupby('Timespan id')['No results'].mean()
                grouped_results.plot(kind='bar', color='green', alpha=0.7)
                plt.title(f"Average Number of Results per Timespan for {db_name}")
                plt.xlabel("Timespan id")
                plt.ylabel("Average Number of Results")
                plt.grid(axis='y')
            else:
                logger.warn(f"No valid 'Timespan id' column in {db_name} for plotting.")
        plt.show()

def compare_bboxes_avg_time(file_patterns, save_location="/"):
    db_dataframes = load_files(file_patterns)

    all_bbox_data = []

    for db_name, df in db_dataframes.items():
        plt.figure(figsize=(12, 6))
        if 'Timespan id' not in df.columns or df['Timespan id'].isna().all():
            if 'Bbox id' in df.columns and df['Bbox id'].notna().any():
                grouped_results = df.groupby('Bbox id')['Query time'].mean()
                grouped_results.plot(kind='bar', color='green', alpha=0.7)
                plt.title(f"Average Time per Bbox for {db_name}")
                plt.xlabel("Bbox id")
                plt.ylabel("Average Time (s)")
                plt.grid(axis='y')

                all_bbox_data.append(grouped_results)
            else:
                logger.warn(f"No valid 'Bbox id' column in {db_name} for plotting.")
        plt.tight_layout()
        plt.savefig(f"{save_location}/{db_name}_time_per_bbox.png")
        plt.show()

    if all_bbox_data:
        overall_avg = pd.concat(all_bbox_data, axis=1).mean(axis=1)
        plt.figure(figsize=(12, 6))
        overall_avg.plot(kind='bar', color='blue', alpha=0.7)
        plt.title("Overall Average Time per Bbox Across All Databases")
        plt.xlabel("Bbox id")
        plt.ylabel("Overall Average Time (s)")
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig(f"{save_location}/time_per_bbox.png")
        plt.show()

def compare_timespans_avg_time(file_patterns, save_location="/"):
    db_dataframes = load_files(file_patterns)

    all_timespan_data = []

    for db_name, df in db_dataframes.items():
        plt.figure(figsize=(12, 6))
        if 'Bbox id' not in df.columns or df['Bbox id'].isna().all():
            if 'Timespan id' in df.columns and df['Timespan id'].notna().any():
                grouped_results = df.groupby('Timespan id')['Query time'].mean()
                grouped_results.plot(kind='bar', color='green', alpha=0.7)
                plt.title(f"Average Time per Timespan for {db_name}")
                plt.xlabel("Timespan id")
                plt.ylabel("Average Time (s)")
                plt.grid(axis='y')

                all_timespan_data.append(grouped_results)
            else:
                logger.warn(f"No valid 'Timespan id' column in {db_name} for plotting.")
        plt.tight_layout()
        plt.savefig(f"{save_location}/{db_name}_time_per_timespan.png")
        plt.show()

    if all_timespan_data:
        overall_avg = pd.concat(all_timespan_data, axis=1).mean(axis=1)
        plt.figure(figsize=(12, 6))
        overall_avg.plot(kind='bar', color='blue', alpha=0.7)
        plt.title("Overall Average Time per Timespan Across All Databases")
        plt.xlabel("Timespan id")
        plt.ylabel("Overall Average Time (s)")
        plt.grid(axis='y')
        plt.tight_layout()
        plt.savefig(f"{save_location}/time_per_timespan.png")
        plt.show()

def calculate_temporal_averages(file_patterns):
    db_dataframes = load_files(file_patterns)

    temporal_averages = {}
    for db_name, df in db_dataframes.items():
        if 'Timespan id' in df.columns and df['Timespan id'].notna().all():
            avg_time = df['Query time'].mean()
            temporal_averages[db_name] = avg_time
        else:
            logger.warn(f"No valid temporal data for {db_name}.")
    return temporal_averages

def calculate_spatial_averages(file_patterns):
    db_dataframes = load_files(file_patterns)

    spatial_averages = {}
    for db_name, df in db_dataframes.items():
        if 'Bbox id' in df.columns and df['Bbox id'].notna().all():
            avg_time = df['Query time'].mean()
            spatial_averages[db_name] = avg_time
        else:
            logger.warn(f"No valid spatial data for {db_name}.")
    return spatial_averages

def calculate_spatiotemporal_averages(file_patterns):
    db_dataframes = load_files(file_patterns)

    spatiotemporal_averages = {}
    for db_name, df in db_dataframes.items():
        if ('Timespan id' in df.columns and df['Timespan id'].notna().all() and
            'Bbox id' in df.columns and df['Bbox id'].notna().all()):
            avg_time = df['Query time'].mean()
            spatiotemporal_averages[db_name] = avg_time
        else:
            logger.warn(f"No valid spatiotemporal data for {db_name}.")
    return spatiotemporal_averages

mongo_temporal_results = os.path.join(MONGO_RESULT_DIR, MONGO_FILENAME + DELIMITER_FILENAME + TEMPORAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
mongo_spatial_results = os.path.join(MONGO_RESULT_DIR, MONGO_FILENAME + DELIMITER_FILENAME + SPATIAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
mongo_spatiotemporal_results = os.path.join(MONGO_RESULT_DIR, MONGO_FILENAME + DELIMITER_FILENAME + SPATIOTEMPORAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
mobility_temporal_results = os.path.join(MOBILITY_RESULT_DIR, MOBILITY_FILENAME + DELIMITER_FILENAME + TEMPORAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
mobility_spatial_results = os.path.join(MOBILITY_RESULT_DIR, MOBILITY_FILENAME + DELIMITER_FILENAME + SPATIAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
mobility_spatiotemporal_results = os.path.join(MOBILITY_RESULT_DIR, MOBILITY_FILENAME + DELIMITER_FILENAME + SPATIOTEMPORAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
influx_temporal_results = os.path.join(INFLUX_RESULT_DIR, INFLUX_FILENAME + DELIMITER_FILENAME + TEMPORAL_FILENAME + WILDCARD + SUFFIX_FILENAME)
SAVE_LOCATION = os.environ.get("SAVE_LOCATION")

# load_combine_and_plot_results(mongo_spatiotemporal_results)

file_patterns_all = {
    "influxdb": [influx_temporal_results],
    "mongodb": [mongo_spatiotemporal_results, mongo_temporal_results, mongo_spatial_results],
    "mobilitydb": [mobility_spatiotemporal_results, mobility_temporal_results, mobility_spatial_results],
}
file_patterns_bboxes = {
    "mongodb": [mongo_spatial_results],
    "mobilitydb": [mobility_spatial_results],
}
file_patterns_timespans = {
    "influxdb": [influx_temporal_results],
    "mongodb": [mongo_temporal_results],
    "mobilitydb": [mobility_temporal_results],
}
file_patterns_spatiotemporal = {
    "mongodb": [mongo_spatiotemporal_results],
    "mobilitydb": [mobility_spatiotemporal_results],
}

# This will show and save the plots
# compare_databases(file_patterns_all, SAVE_LOCATION)
# compare_bboxes_avg_time(file_patterns_bboxes, SAVE_LOCATION)
# compare_timespans_avg_time(file_patterns_timespans, SAVE_LOCATION)

# This will print the final results
space = calculate_spatial_averages(file_patterns_bboxes)
time = calculate_temporal_averages(file_patterns_timespans)
spacetime = calculate_spatial_averages(file_patterns_spatiotemporal)

print(f"Space: {space}")
print(f"Time: {time}")
print(f"Spacetime: {spacetime}")

