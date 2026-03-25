def analyze_city(city_df):
    city_df = city_df.sort_values(by="timestamp")
    city_df["temp_rolling"] = (city_df["temperature"].rolling(window=30, min_periods=1).mean())

    seasons_data = (city_df.groupby("season")["temperature"].agg(["mean", "std"]).reset_index().rename(columns={"mean": "season_mean", "std": "season_std"}))
    seasons_data["city"] = city_df["city"].iloc[0]

    city_df = city_df.merge(seasons_data, on=["city", "season"], how="left")
    city_df["anomaly"] = (city_df["temperature"] > city_df["season_mean"] + 2 * city_df["season_std"]) | (city_df["temperature"] < city_df["season_mean"] - 2 * city_df["season_std"])

    return city_df, seasons_data, city_df[city_df["anomaly"]]
