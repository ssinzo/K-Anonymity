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

    with open('./data_result/' + file_name + '.csv') as f:
        reader = csv.reader(f, dialect='excel', delimiter='\t')
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'condition':
                    ret = []
                    for val in v.split(','):
                        val_recover = dict_condition.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'ethnic':
                    ret = []
                    for val in v.split(','):
                        val_recover = dict_ethnic.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'gender':
                    ret = []
                    for val in v.split(','):
                        val_recover = dict_gender.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'race':
                    ret = []
                    for val in v.split(','):
                        val_recover = dict_race.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'birthdate':
                    ret = []
                    for val in v.split(','):
                        val_recover = dict_birthdate.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = '{x}-{y}'.format(x=val_recover[0], y=val_recover[1])

                columns[h].append(v)
    return pd.DataFrame(columns)


distance_birthdate = load_distance('birthdate')
distance_condition = load_distance('condition')
distance_ethnic = load_distance('ethnic')
distance_gender = load_distance('gender')
distance_race = load_distance('race')

df = load_data("data_result", distance_birthdate, distance_condition, distance_ethnic, distance_gender, distance_race)

df.to_csv('./data_recover/data_recover.csv', encoding='utf-8', index=False)
