import pandas as pd
import numpy as np

df = pd.read_csv("temperature_data.csv", parse_dates=["timestamp"])


def analyzing_temp(df):

    df = df.sort_values(by=["city", "timestamp"])
    df["temp_rolling"] = df.groupby("city")["temperature"].transform(
        lambda x: x.rolling(window=30, min_periods=1).mean()
    )  # Скользящее среднее
    seasons_data = (
        df.groupby(["city", "season"])["temperature"]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={"mean": "season_mean", "std": "season_std"})
    )  # Среднее и стандартное отклонения
    df = df.merge(seasons_data, on=["city", "season"], how="left")
    df["anomaly"] = (df["temperature"] > df["season_mean"] + 2 * df["season_std"]) | (
        df["temperature"] < df["season_mean"] - 2 * df["season_std"]
    )
    anomaly = df[df["anomaly"]]

    return df, seasons_data, anomaly


from multiprocessing import Pool, cpu_count
from analyze_city import *


def analyzing_temp_parallel(df, n_workers=None):
    if n_workers is None:
        n_workers = min(cpu_count(), df["city"].nunique())

    city_groups = []
    for _, group in df.groupby("city"):
        city_groups.append(group)

    with Pool(n_workers) as pool:
        results = pool.map(analyze_city, city_groups)

    dfs = []
    seasons = []
    anomalies = []

    for result in results:
        df_part = result[0]
        season_part = result[1]
        anomaly_part = result[2]

        dfs.append(df_part)
        seasons.append(season_part)
        anomalies.append(anomaly_part)

    all_df = pd.concat(dfs, ignore_index=True)
    all_seasons = pd.concat(seasons, ignore_index=True)
    all_anomalies = pd.concat(anomalies, ignore_index=True)

    return all_df, all_seasons, all_anomalies
