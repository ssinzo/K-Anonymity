import pandas as pd
import csv
from datetime import datetime

feature_columns = ['AGE', 'RACE', 'ETHNICITY', 'GENDER', 'BIRTHPLACE', 'CONDITION']
categorical_feature_columns = ['RACE', 'ETHNICITY', 'GENDER', 'BIRTHPLACE', 'CONDITION']
sensitive_column = 'DEATH'


def agg_numerical_column(series):
    return ['{x},{y}'.format(x=series.min(), y=series.max())]


def agg_categorical_column(series):
    val = []

    for x in series.values:
        if x not in val:
            val.append(x)

    ret = ','.join([str(x) for x in val])

    return ret


def build_anonymity_dataset(df, partitions, max_partitions=None):
    aggregations = {}

    for column in feature_columns:
        if column in categorical_feature_columns:
            aggregations[column] = agg_categorical_column
        else:
            aggregations[column] = agg_numerical_column

    rows = []
    save_columns = None

    for i, partition in enumerate(partitions):

        if not i % 1000:
            print("{t} : Finished {i} partitions...".format(i=i, t=datetime.now().strftime('%H:%M:%S')))

        if max_partitions is not None and i > max_partitions:
            break

        grouped_columns = df.loc[partition].agg(aggregations, squeeze=False)

        sensitive_counts = df.loc[partition].groupby(sensitive_column).agg({sensitive_column: 'count'})

        for sensitive_value, count in sensitive_counts[sensitive_column].items():
            if count < 3:
                continue

            grouped_columns[sensitive_column] = sensitive_value
            grouped_columns['count'] = count

            rows.append(data[0] for data in grouped_columns.to_dict('list').values())
            save_columns = grouped_columns.columns

    print("{t} : Finished partitions...".format(t=datetime.now().strftime('%H:%M:%S')))

    return pd.DataFrame(rows, columns=save_columns)


df = pd.read_csv("./data_convert/data_convert.csv", sep=",", engine='python')

with open('./data_partition/ssinzo_k_anonymity.csv', 'r') as f:

    st_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    reader = csv.reader(f)

    rows = []
    for row in reader:
        cols = []
        for col in row:
            cols.append(int(col))
        rows.append(cols)

    finished_partitions = rows

    print('total partition len : {len}'.format(len= len(finished_partitions)))

    finished_df = build_anonymity_dataset(df, finished_partitions)

    ed_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    print("============ result ============")
    print("st_time : {}".format(st_timestamp))
    print("ed_time : {}".format(ed_timestamp))
    print("================================")

    finished_df.to_csv('./data_result/data_result.csv', sep='\t', index=False)
