#!/usr/bin/env python3

import pandas as pd

from storage import save_to_parquet
from validators import validate_flight


def add_flight(df, destination, flight_number, aircraft_type):
    if not validate_flight(destination, flight_number, aircraft_type):
        return df

    flight_number = str(flight_number).strip()
    if flight_number in df["flight_number"].values:
        print(f"❌ Ошибка: Рейс с номером {flight_number} уже существует!")
        return df
    new_data = pd.DataFrame(
        [
            {
                "destination": destination.strip(),
                "flight_number": flight_number,
                "aircraft_type": aircraft_type.strip(),
            }
        ]
    )
    df = pd.concat([df, new_data], ignore_index=True)
    print(f"✅ Рейс {flight_number} в {destination} добавлен.")
    save_to_parquet(df)
    return df


def show_flights(df):
    print("📋 Текущие рейсы:")
    print(df if not df.empty else "❌ Список рейсов пуст.")


def remove_flight(df, flight_number):
    flight_number = str(flight_number).strip()

    if df.empty:
        print("❌ Список рейсов пуст. Удаление невозможно.")
        return df

    if flight_number not in df["flight_number"].values:
        print(f"❌ Рейс с номером {flight_number} не найден.")
        return df

    initial_count = len(df)
    df = df[df["flight_number"] != flight_number]
    removed_count = initial_count - len(df)

    print(f"✅ Удалено {removed_count} рейсов с номером {flight_number}.")
    save_to_parquet(df)
    return df
