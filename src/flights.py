import pandas as pd
from validators import validate_flight
from storage import save_to_parquet

def add_flight(df, destination, flight_number, aircraft_type):
    if not validate_flight(destination, flight_number, aircraft_type):
        return df

    flight_number = str(flight_number).strip()
    if flight_number in df["flight_number"].values:
        print(f"❌ Ошибка: Рейс с номером {flight_number} уже существует!")
        return df  # Возвращаем исходный DataFrame без изменений
    new_data = pd.DataFrame([{
        "destination": destination.strip(),
        "flight_number": flight_number,
        "aircraft_type": aircraft_type.strip()
    }])
    df = pd.concat([df, new_data], ignore_index=True)
    print(f"✅ Рейс {flight_number} в {destination} добавлен.")
    save_to_parquet(df)
    return df

def show_flights(df):
    print("📋 Текущие рейсы:")
    print(df if not df.empty else "❌ Список рейсов пуст.")
