import pandas as pd
from datetime import datetime
import csv


feature_columns = ['birthdate', 'gender', 'race', 'ethnic', 'condition']
sensitive_column = 'death'


def get_spans(df, partition, scale=None):
    spans = {}

    for column in df.columns:
        span = (df[column][partition].max() - df[column][partition].min()) / 2

        if scale is not None:
            span = span / scale[column]

        spans[column] = span

    return spans


def split(df, partition, column):
    dfp = df[column][partition]
    median = dfp.median()
    dfl = dfp.index[dfp <= median]
    dfr = dfp.index[dfp > median]

    return (dfl, dfr)


def is_k_anonymous(partition, k=3):
    return False if len(partition) < k else True


def partition_dataset(df, feature_columns, is_valid):
    finished_partitions = []
    partitions = [df.index]

    idx = 0

    while partitions:
        partition = partitions.pop(0)
        spans = get_spans(df[feature_columns], partition)

        for column, span in sorted(spans.items(), key= lambda x: x[1]): #낮은값 높은값
            lp, rp = split(df, partition, column)

            if not idx % 1000 :
                print("{idx} : {column} [lp.len : {x}, rp.len : {y}]".format(idx= idx, column= column, x= len(lp), y= len(rp)))
            idx = idx + 1

            if not is_valid(lp) or not is_valid(rp):
                continue

            partitions.extend((lp, rp))
            break
        else:
            finished_partitions.append(partition)

    return finished_partitions


st_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

df = pd.read_csv("./data_convert/data_convert.csv", sep="\t", engine='python');

finished_partitions = partition_dataset(df, feature_columns, is_k_anonymous)

ed_timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

print("============ result ============")
print("st_time : {}".format(st_timestamp))
print("ed_time : {}".format(ed_timestamp))
print("finished_partition len : {len}".format(len= len(finished_partitions)))
print("================================")

with open('./data_partition/ssinzo_k_anonymity.csv', 'w') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)

    idx = 0
    for partition in finished_partitions:
        wr.writerow(partition)
        idx += 1

        if not idx%100 :
            print("complete partition at {x}".format(x=idx))