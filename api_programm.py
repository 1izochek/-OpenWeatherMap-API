import pandas as pd
from my_functions import analyzing_temp  # импорт функции из другого файла
import requests
from datetime import datetime
import aiohttp
import asyncio


API_KEY = "секрет"

df_hist = pd.read_csv("temperature_data.csv", parse_dates=["timestamp"])
df_hist, stats, anomalies = analyzing_temp(df_hist)

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

def get_current_temperature(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric"}
    response = requests.get(url, params=params)
    data = response.json()
    return data["main"]["temp"]


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

cities = ["Moscow", "Beijing", "Cairo", "Dubai", "Berlin"]

for city in cities:
    temp = get_current_temperature(city)
    status = check_temperature_anomaly(city, temp, stats)
    print(f"Синхронная: {city}: {temp}C => {status}")


# Вывело вот это и кстати в Москве сейчас реально 13 градусов: Moscow: 12.42C => Норма

async def get_current_temperature_async(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": API_KEY, "units": "metric"}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            return data["main"]["temp"]


async def check_city_async(city):
    temp = await get_current_temperature_async(city)
    status = check_temperature_anomaly(city, temp, stats)
    print(f"Асинхронная: {city}: {temp}C => {status}")


async def main():
    temp = await check_city_async("Moscow")
    print(temp)


async def main():
    temp = await check_city_async("Berlin")
    print(temp)


async def main():
    temp = await check_city_async("Dubai")
    print(temp)


async def main():
    cities = ["Moscow", "Beijing", "Cairo", "Dubai", "Berlin"]
    tasks = [check_city_async(city) for city in cities]
    await asyncio.gather(*tasks)


asyncio.run(main())

"""Синхронная: Moscow: 11.36C => Норма
Синхронная: Beijing: 11.94C => Норма
Синхронная: Cairo: 16.42C => Норма
Синхронная: Dubai: 22.45C => Норма
Синхронная: Berlin: 10.86C => Норма
Асинхронная: Moscow: 11.36C => Норма
Асинхронная: Beijing: 11.94C => Норма
Асинхронная: Berlin: 10.86C => Норма
Асинхронная: Cairo: 16.42C => Норма
Асинхронная: Dubai: 22.45C => Норма"""

# Я думаю, что у меня в датасете всего 15 городов и мне не обязательно использовать асинхронность, но если бы городов было 100 или 1000, то асинхронность помогла бы значительно сократить время ответа. В данном случае, синхронный код выполняется достаточно быстро, так как количество запросов небольшое. 