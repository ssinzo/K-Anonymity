import pandas as pd
import csv


def load_distance(coloum_name):
    distance_dict = dict()
    with open('./data_distance/distance_' + coloum_name + ".csv", newline='') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            distance_dict.update({round(float(row[2]),5): row[1]})
    return distance_dict


def load_data(file_name):

    columns = {}
    distance_age = load_distance('AGE')
    distance_race = load_distance('RACE')
    distance_ethnicity = load_distance('ETHNICITY')
    distance_gender = load_distance('GENDER')
    distance_birthplace = load_distance('BIRTHPLACE')
    distance_condition = load_distance('CONDITION')

    with open('./data_result/' + file_name + '.csv') as f:
        reader = csv.reader(f, dialect='excel', delimiter='\t')
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'CONDITION':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_condition.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'RACE':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_race.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'ETHNICITY':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_ethnicity.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'GENDER':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_gender.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'BIRTHPLACE':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_birthplace.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'AGE':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_age.get(round(float(val), 5))
                        ret.append(val_recover)
                    v = '{x}-{y}'.format(x=ret[0], y=ret[1])

                columns[h].append(v)
    return pd.DataFrame(columns)

df = load_data("data_result")

df.to_csv('./data_recover/data_recover.csv', encoding='utf-8', index=False)
