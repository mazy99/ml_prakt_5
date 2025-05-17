#!/usr/bin/env python3

import pandas as pd

DEFAULT_FILE = "flights.parquet"


def save_to_parquet(df, file_name=DEFAULT_FILE):
    if df.empty:
        print("⚠️ Внимание: Вы пытаетесь сохранить пустой список рейсов!")
    else:
        print("📅 Сохранение данных...")
        df.to_parquet(file_name, engine="pyarrow")
        print(f"✅ Данные сохранены в {file_name}")


def load_from_parquet(file_name=DEFAULT_FILE, destination=None):
    try:
        df_loaded = pd.read_parquet(file_name, engine="pyarrow").copy()

        if set(df_loaded.columns) != {"destination", "flight_number", "aircraft_type"}:
            print(f"❌ Ошибка: Некорректная структура файла {file_name}.")
            return pd.DataFrame()

        if destination:
            df_loaded = df_loaded[df_loaded["destination"] == destination]

        return df_loaded
    except Exception as e:
        print(f"❌ Ошибка чтения Parquet: {e}")
        return pd.DataFrame()
