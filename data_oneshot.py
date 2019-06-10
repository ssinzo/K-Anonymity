import subprocess
from datetime import datetime

time_stamp = list()

time_stamp.append(['distance', datetime.now().strftime('%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_distance.py'], shell=True)

time_stamp.append(['convert', datetime.now().strftime('%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_convert.py'], shell=True)

time_stamp.append(['partition', datetime.now().strftime('%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_partition.py'], shell=True)

time_stamp.append(['build', datetime.now().strftime('%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_build.py'], shell=True)

time_stamp.append(['recover', datetime.now().strftime('%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_recover.py'], shell=True)

time_stamp.append(['recover_count', datetime.now().strftime('%%Y/%m/%d - %H:%M:%S')])
subprocess.run(['python ./data_recover_count.py'], shell=True)


for step in time_stamp:
    print(step)
