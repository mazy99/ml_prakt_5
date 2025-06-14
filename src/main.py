#!/usr/bin/env python3

import argparse
import os

import pandas as pd

from flights import add_flight, remove_flight, show_flights
from storage import DEFAULT_FILE, load_from_parquet, save_to_parquet


def main():
    df = pd.DataFrame(columns=["destination", "flight_number", "aircraft_type"])

    if os.path.exists(DEFAULT_FILE):
        try:
            df = pd.read_parquet(DEFAULT_FILE, engine="pyarrow").copy()
        except Exception as e:
            print(f"⚠️ Не удалось загрузить данные при запуске: {e}")

    parser = argparse.ArgumentParser(description="Управление рейсами")
    subparsers = parser.add_subparsers(dest="command", help="Выберите команду")

    parser_add = subparsers.add_parser("add", help="Добавить рейс")
    parser_add.add_argument("destination", type=str)
    parser_add.add_argument("flight_number", type=str)
    parser_add.add_argument("aircraft_type", type=str)

    subparsers.add_parser("save", help="Сохранить данные")

    parser_load = subparsers.add_parser("load", help="Загрузить данные по городу")
    parser_load.add_argument("destination", type=str, nargs="?", default=None)

    parser_remove = subparsers.add_parser("remove", help="Удалить рейс по номеру")
    parser_remove.add_argument(
        "flight_number", type=str, help="Номер рейса для удаления"
    )

    subparsers.add_parser("show", help="Показать все рейсы")

    args = parser.parse_args()

    if args.command == "add":
        df = add_flight(df, args.destination, args.flight_number, args.aircraft_type)
    elif args.command == "save":
        save_to_parquet(df)
    elif args.command == "load":
        loaded_df = load_from_parquet(destination=args.destination)
        print(
            loaded_df
            if not loaded_df.empty
            else "❌ Рейсов по заданному направлению не найдено."
        )
    elif args.command == "show":
        show_flights(df)
    elif args.command == "remove":
        df = remove_flight(df, args.flight_number)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
