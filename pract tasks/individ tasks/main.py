import argparse
import os
import re

import pandas as pd

DEFAULT_FILE = "flights.parquet"

df = pd.DataFrame(columns=["destination", "flight_number", "aircraft_type"])


def validate_flight(destination, flight_number, aircraft_type):
    if not destination.strip():
        print("❌ Ошибка: Город назначения не может быть пустым.")
        return False

    flight_number = str(flight_number).strip()
    if not re.match(r"^\d+$", flight_number):
        print("❌ Ошибка: Номер рейса должен содержать только цифры.")
        return False

    if not aircraft_type.strip():
        print("❌ Ошибка: Тип самолета не может быть пустым.")
        return False

    return True


def add_flight(destination, flight_number, aircraft_type):
    global df

    # Загружаем существующие данные, если файл существует
    if os.path.exists(DEFAULT_FILE):
        try:
            df = pd.read_parquet(DEFAULT_FILE, engine="pyarrow").copy()
        except Exception as e:
            print(f"⚠️ Ошибка загрузки данных: {e}")
            df = pd.DataFrame(columns=["destination", "flight_number", "aircraft_type"])

    if not validate_flight(destination, flight_number, aircraft_type):
        return

    flight_number = str(flight_number).strip()
    new_data = pd.DataFrame([{
        "destination": destination.strip(),
        "flight_number": flight_number,
        "aircraft_type": aircraft_type.strip()
    }])
    df = pd.concat([df, new_data], ignore_index=True)

    print(f"✅ Рейс {flight_number} в {destination} добавлен.")
    save_to_parquet()


def save_to_parquet(file_name=DEFAULT_FILE):
    global df

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
            return

        if destination:
            df_loaded = df_loaded[df_loaded["destination"] == destination]

        if not df_loaded.empty:
            print(df_loaded)
        else:
            print("❌ Рейсов по заданному направлению не найдено.")
    except Exception as e:
        print(f"❌ Ошибка чтения Parquet: {e}")


def show_flights():
    global df

    print("📋 Текущие рейсы:")
    print(df if not df.empty else "❌ Список рейсов пуст.")


def main():
    global df

    # Загружаем данные при запуске
    if os.path.exists(DEFAULT_FILE):
        try:
            df = pd.read_parquet(DEFAULT_FILE, engine="pyarrow").copy()
        except Exception as e:
            print(f"⚠️ Не удалось загрузить данные при запуске: {e}")

    parser = argparse.ArgumentParser(description="Управление рейсами")
    subparsers = parser.add_subparsers(dest="command", help="Выберите команду")

    parser_add = subparsers.add_parser("add", help="Добавить рейс")
    parser_add.add_argument("destination", type=str, help="Город назначения")
    parser_add.add_argument("flight_number", type=str, help="Номер рейса")
    parser_add.add_argument("aircraft_type", type=str, help="Тип самолета")

    subparsers.add_parser("save", help="Сохранить данные")

    parser_load = subparsers.add_parser("load", help="Загрузить данные по городу")
    parser_load.add_argument("destination", type=str, nargs="?", default=None,
                             help="Город назначения (необязательно)")

    subparsers.add_parser("show", help="Показать все рейсы")

    args = parser.parse_args()

    if args.command == "add":
        add_flight(args.destination, args.flight_number, args.aircraft_type)
    elif args.command == "save":
        save_to_parquet()
    elif args.command == "load":
        load_from_parquet(destination=args.destination)
    elif args.command == "show":
        show_flights()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
