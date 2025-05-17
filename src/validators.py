#!/usr/bin/env python3

import re

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
