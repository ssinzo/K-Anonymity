import csv


def readCSV(path, sep=',', isFirstSkip=False):
    csvList = list()
    with open(path, 'r', encoding='utf-8') as lines:
        for line in lines:
            if isFirstSkip:
                isFirstSkip = False
                continue

            cells = line.split(sep)
            cells[-1] = cells[-1].replace('\n', '')
            csvList.append(cells)

    return csvList


def recover(filename, dataset):
    with open('./data_recover_count/' + filename + '.csv', 'w') as myfile:
        wr = csv.writer(myfile, delimiter='\t')

        save_list = list()
        save_list.append(dataset[0][:len(dataset[0]) - 1])

        temp = dataset[1:]

        for row in temp:

            cnt = int(row[-1])

            if cnt < 3:
                continue

            for i in range(cnt):
                save_list.append(row[:len(row) - 1])

        for row in save_list:
            wr.writerow(row)


mond_dataset_ori  = readCSV('./data_recover/original_moned_DataSet.csv', sep='\t')
mond_dataset_bert = readCSV('./data_recover/recover_bert.csv', sep='\t')

recover('recover_origin_wc', mond_dataset_ori)
recover('recover_bert_wc', mond_dataset_bert)
