from scipy import spatial
import csv

feature_columns = ['AGE', 'RACE', 'ETHNICITY', 'GENDER', 'BIRTHPLACE', 'CONDITION']

for column in feature_columns:

    data = []

    with open('./data_embedding/embedding_' + column + '.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        for row in reader:
            row_data = []
            for i in range(len(row)):
                if i == 0:
                    row_data.append(str(row[i]))
                else:
                    row_data.append(float(row[i]))
            data.append(row_data)

    key_list = []
    val_list = []
    for i in range(len(data)):
        key_list.append(data[i][0])
        val_list.append(data[i][1:])

    x = key_list[0]
    x_bert = val_list[0]

    print('save {x} start'.format(x=column))

    with open('./data_distance/distance_' + column + '.csv', 'w') as myfile:
        wr = csv.writer(myfile, delimiter='\t')

        for y, y_bert in zip(key_list, val_list):
            temp = []
            temp.append(x)
            temp.append(y)
            print(x_bert)
            print(y_bert)
            print(1 - spatial.distance.cosine(x_bert, y_bert))
            temp.append(round(1 - spatial.distance.cosine(x_bert, y_bert), 6))

            wr.writerow(temp)

            print("complete : {x}".format(x=y))

    print('complete bert_{x} embedding'.format(x=column))
