import streamlit as st
import pandas as pd
import numpy as np
import requests
import plotly.express as px
from datetime import datetime


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

    return df, seasons_data, df[df["anomaly"]]


def get_season():
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"


def check_temperature_anomaly(city, current_temp, stats):
    season = get_season()
    row = stats[(stats["city"] == city) & (stats["season"] == season)]
    if row.empty:
        return "Нет данных"
    mean = row["season_mean"].values[0]
    std = row["season_std"].values[0]
    lower = mean - 2 * std
    upper = mean + 2 * std
    if current_temp < lower or current_temp > upper:
        return "Аномалия"
    else:
        return "Норма"


def get_current_temperature(city, api_key):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}
    response = requests.get(url, params=params)
    data = response.json()
    if response.status_code != 200:
        return None, data  
    return data["main"]["temp"], None

st.title("Weather Analysis App")
st.write("Здравствуйте, дорогие пользователи и Дима! В этом приложении вы можете загрузить исторические данные о температуре, проанализировать их и сравнить с текущими данными из OpenWeatherMap Api. ^-^")
st.image("https://i.pinimg.com/736x/d1/f9/19/d1f919d11670c01751c051b77a97ca99.jpg", caption="Зачем приложение, когда есть болт")

historic_file = st.file_uploader("Пожалуйста, загрузите файл с историческими данными в формате .csv в это поле", type="csv")
if historic_file is not None:
    df = pd.read_csv(historic_file, parse_dates=["timestamp"])
    st.success("Ваши данные успешно загружены!")
    cities = df["city"].unique()
    city = st.selectbox("Пожалуйста, выберите город из предложенных", cities)

    api_key = st.text_input("Пожалуйста, введите свой OpenWeatherMap API Key", type="password")

    df_processed, stats, anomalies = analyzing_temp(df)
    city_data = df_processed[df_processed["city"] == city]
    city_stats = stats[stats["city"] == city]

    st.subheader(f"Описательная статистика для {city}")
    st.write(city_stats)

    st.subheader("Временной ряд температур с аномалиями")
    fig = px.line(
        city_data, x="timestamp", y="temperature", title=f"{city} - температуры"
    )
    fig.add_scatter(
        x=city_data[city_data["anomaly"]]["timestamp"],
        y=city_data[city_data["anomaly"]]["temperature"],
        mode="markers",
        marker=dict(color="red", size=8),
        name="Аномалии",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Сезонные профили")
    fig_season = px.bar(
        city_stats,
        x="season",
        y="season_mean",
        error_y="season_std",
        labels={
            "season_mean": "Средняя температура",
            "season_std": "Стандартное отклонение",
        },
    )
    st.plotly_chart(fig_season, use_container_width=True)

    if api_key:
        temp, error = get_current_temperature(city, api_key)
        if temp is not None:
            status = check_temperature_anomaly(city, temp, stats)
            st.subheader(f"Текущая температура в {city}: {temp}C => {status}")
        else:
            st.error(f"Ошибка API: {error}")
    else:
        st.info("Введите, пожалуйста, API Key для получения текущей погоды.")
else:
    st.info("Загрузите, пожалуйста, CSV для начала анализа.")
