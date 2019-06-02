import pandas as pd
import csv


def load_distance(coloum_name):
    distance_dict = dict()
    with open('./data_distance/distance_' + coloum_name + ".csv", newline='') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            distance_dict.update({row[1]: row[2]})
    return distance_dict


def load_data(file_name):

    distance_age = load_distance('AGE')
    distance_race = load_distance('RACE')
    distance_ethnicity = load_distance('ETHNICITY')
    distance_gender = load_distance('GENDER')
    distance_birthplace = load_distance('BIRTHPLACE')
    distance_condition = load_distance('CONDITION')

    columns = {}

    with open('./data/' + file_name + '.csv') as f:
        reader = csv.reader(f, dialect='excel', delimiter='\t')
        headers = next(reader, None)

        for h in headers:
            columns[h] = []

        for row in reader:
            for h, v in zip(headers, row):
                if h == 'CONDITION':
                    v = distance_condition.get(v)
                elif h == 'BIRTHPLACE':
                    v = distance_birthplace.get(v)
                elif h == 'GENDER':
                    v = distance_gender.get(v)
                elif h == 'ETHNICITY':
                    v = distance_ethnicity.get(v)
                elif h == 'RACE':
                    v = distance_race.get(v)
                elif h == 'AGE':
                    v = distance_age.get(str(v))

                columns[h].append(v)
    return pd.DataFrame(columns)


df = load_data("finalPatientDataSet")
df.to_csv('./data_convert/data_convert.csv', encoding='utf-8', index=False)

