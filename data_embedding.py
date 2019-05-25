import pandas as pd
import csv
from datetime import datetime, date
from bert_serving.client import BertClient


feature_columns = ['birthdate', 'gender', 'race', 'ethnic', 'condition']


bc = BertClient()
df = pd.read_csv("./data/finalConditionInfo.csv", sep=",", index_col=False, engine='python')


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


for feature_column in feature_columns:

    if feature_column == 'birthdate':
        rows = []

        for birth in df[feature_column]:
            rows.append({str(calculate_age(datetime.strptime(birth, '%Y-%m-%d')))})

        df_bir = pd.DataFrame(rows, columns=[feature_column])
        column_list = df_bir[feature_column].unique().tolist()
    else:
        v = df[feature_column].unique().tolist()
        column_list = [str.lower(feature.replace('-', ' ')) for feature in v]

    bert_list = bc.encode(column_list).tolist()

    print('save {x} start'.format(x=feature_column))

    with open('./data_embedding/bert_' + feature_column + '.csv', 'w') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)

        for x, x_bert in zip(column_list, bert_list):
            temp = []
            temp.append(x)

            for bert in x_bert:
                temp.append(bert)

            wr.writerow(temp)
            print("complete {x}".format(x=x))

    print('complete bert_{x} embedding'.format(x=feature_column))
