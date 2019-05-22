import pandas as pd
import csv
from datetime import datetime


feature_columns = ['birthdate', 'gender', 'race', 'ethnic', 'condition']
sensitive_column = 'death'


def agg_categorical_column(series):
    return [','.join(set(series))]


def agg_numerical_column(series):
    return [series.max()]


def build_anonymized_dataset(df, partitions, feature_columns, sensitive_column, max_partitions=None):
    aggregations = {}

    for column in feature_columns:
        aggregations[column] = agg_numerical_column

    rows = []

    for i, partition in enumerate(partitions):

        if not i % 1000:
            print("Finished {} partitions...".format(i))

        if max_partitions is not None and i > max_partitions:
            break

        grouped_columns = df.loc[partition].agg(aggregations, squeeze=False)

        sensitive_counts = df.loc[partition].groupby(sensitive_column).agg({sensitive_column: 'count'})

        values = grouped_columns.iloc[0].to_dict()

        for sensitive_value, count in sensitive_counts[sensitive_column].items():
            if count == 0:
                continue

            values.update({
                sensitive_column: sensitive_value,
                'count': count,

            })
            rows.append(values.copy())
    return pd.DataFrame(rows)


st_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

df = pd.read_csv("./data_convert/data_convert.csv", sep="\t", engine='python');

with open('./data_partition/ssinzo_k_anonymity.csv', 'r') as f:
    reader = csv.reader(f)

    rows = []
    for row in reader:
        cols = []
        for col in row:
            cols.append(int(col))
        rows.append(cols)

    finished_partitions = rows

    print('total partioin len : {len}'.format(len= len(finished_partitions)))

    dfn = build_anonymized_dataset(df, finished_partitions, feature_columns, sensitive_column)

    ed_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    print("============ result ============")
    print("st_time : {}".format(st_timestamp))
    print("ed_time : {}".format(ed_timestamp))
    print("================================")

    dfn.to_csv('./data_result/data_result.csv', encoding='utf-8', index=False)
