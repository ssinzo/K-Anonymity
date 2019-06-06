import pandas as pd
import csv
from bert_serving.client import BertClient

bc = BertClient()
feature_columns = ['AGE', 'RACE', 'ETHNICITY', 'GENDER', 'BIRTHPLACE', 'CONDITION']

df = pd.read_csv("./data/finalPatientDataSet_20.csv", sep="\t", index_col=False, engine='python')

for feature_column in feature_columns:

    # DEATH > 최빈값
    v = df['DEATH'].groupby(df[feature_column]).sum().sort_values(ascending=False).keys()

    column_list = [str(feature) for feature in v]

    bert_list = bc.encode(column_list).tolist()

    print('save {x} start'.format(x=feature_column))

    with open('./data_embedding/embedding_' + feature_column + '.csv', 'w') as myfile:
        wr = csv.writer(myfile, delimiter='\t')

        for x, x_bert in zip(column_list, bert_list):
            temp = []
            temp.append(x)

            for bert in x_bert:
                temp.append(bert)

            wr.writerow(temp)
            print("complete {x}".format(x=x))

    print('complete bert_{x} embedding'.format(x=feature_column))
