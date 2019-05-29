import pandas as pd
import csv


def load_distance(coloum_name):
    distance_dict = dict()
    with open('./data_distance/distance_' + coloum_name + ".csv", newline='') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            distance_dict.update({round(float(row[2]),5): row[1]})
    return distance_dict


def load_data(file_name, dict_birthdate, dict_condition, dict_ethnic, dict_gender, dict_race):
    columns = {}

    with open('./data_result/' + file_name + '.csv', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'condition':
                    v = dict_condition.get(round(float(v), 5))
                elif h == 'ethnic':
                    v = dict_ethnic.get(round(float(v), 5))
                elif h == 'gender':
                    v = dict_gender.get(round(float(v), 5))
                elif h == 'race':
                    v = dict_race.get(round(float(v), 5))
                elif h == 'birthdate':
                    v = dict_birthdate.get(round(float(v), 5))

                columns[h].append(v)
    return pd.DataFrame(columns)

distance_birthdate = load_distance('birthdate')
distance_condition = load_distance('condition')
distance_ethnic = load_distance('ethnic')
distance_gender = load_distance('gender')
distance_race = load_distance('race')

df = load_data("data_result", distance_birthdate, distance_condition, distance_ethnic, distance_gender, distance_race)

df.to_csv('./data_recover/data_recover.csv', encoding='utf-8', index=False)
