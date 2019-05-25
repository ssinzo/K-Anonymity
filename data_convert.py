import pandas as pd
import csv
from datetime import datetime, date


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def load_distance(coloum_name):
    distance_dict = dict()
    with open('./data_distance/distance_' + coloum_name + ".csv", newline='') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            distance_dict.update({row[1]: row[2]})
    return distance_dict


def load_data(file_name, distance_birthdate, dict_condition, dict_ethnic, dict_gender, dict_race):
    columns = {}

    with open('./data/' + file_name + '.csv', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'condition':
                    v = dict_condition.get(str.lower(v.replace('-', ' ')))
                elif h == 'ethnic':
                    v = dict_ethnic.get(str.lower(v.replace('-', ' ')))
                elif h == 'gender':
                    v = dict_gender.get(str.lower(v.replace('-', ' ')))
                elif h == 'race':
                    v = dict_race.get(str.lower(v.replace('-', ' ')))
                elif h == 'birthdate':
                    v = distance_birthdate.get(calculate_age(datetime.strptime(v, '%Y-%m-%d')))
                columns[h].append(v)
    return pd.DataFrame(columns)


distance_condition = load_distance('condition')
distance_ethnic = load_distance('ethnic')
distance_gender = load_distance('gender')
distance_race = load_distance('race')
distance_birthdate = load_distance('birthdate')


df = load_data("finalConditionInfo", distance_birthdate, distance_condition, distance_ethnic, distance_gender, distance_race)

df.to_csv('./data_convert/data_convert.csv', encoding='utf-8', index=False)

