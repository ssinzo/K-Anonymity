import pandas as pd
import csv


def load_distance(coloum_name):
    distance_dict = dict()
    with open('./data_distance/distance_' + coloum_name + ".csv", newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        for row in reader:
            distance_dict.update({row[2] : row[1]})
    return distance_dict


def load_data(file_name):

    columns = {}
    distance_race = load_distance('RACE')
    distance_ethnicity = load_distance('ETHNICITY')
    distance_gender = load_distance('GENDER')
    distance_birthplace = load_distance('BIRTHPLACE')
    distance_condition = load_distance('CONDITION')

    with open('./data_build/' + file_name + '.csv') as f:
        reader = csv.reader(f, delimiter='\t')
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'CONDITION':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_condition.get(val)
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'RACE':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_race.get(val)
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'ETHNICITY':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_ethnicity.get(val)
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'GENDER':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_gender.get(val)
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'BIRTHPLACE':
                    ret = []
                    for val in v.split(','):
                        val_recover = distance_birthplace.get(val)
                        ret.append(val_recover)
                    v = ','.join(ret)
                elif h == 'AGE':
                    ret = []
                    for val in v.split(','):
                        val_recover = val
                        ret.append(val_recover)
                    v = '{x}-{y}'.format(x=min(ret), y=max(ret))

                columns[h].append(v)
    return pd.DataFrame(columns)

df = load_data("build_bert")

df.to_csv('./data_recover/recover_bert.csv', sep='\t', encoding='utf-8', index=False)
