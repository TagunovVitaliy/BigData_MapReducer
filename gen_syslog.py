import sys
from datetime import datetime
from os import path
import os
import argparse
import random
import shutil


def generate_malformed_row():
    return str(datetime.now()) + 'malformed'


def generate_devices(num):
    return {i: 'dev_' + str(i) for i in range(num)}


def generate_log_str(dev, stamp):
    return str(dev)+','+str(stamp*1000)+','+str(random.random()*100)+'\n'


def main():
    parser = argparse.ArgumentParser(description='Generating log files')
    parser.add_argument('--start-date', type=str,
                        help='Start date: dd.mm.yyyy')
    parser.add_argument('--end-date', type=str,
                        help='End date: dd.mm.yyyy')
    parser.add_argument('--output', type=str,
                        help='Output directory', default='./logs')
    parser.add_argument('--num-files', type=int,
                        help='Number of files with logs', default=1)
    parser.add_argument('--incorrect', type=int,
                        help='Percent of incorrect data', default=5)

    options = parser.parse_args()

    if not options.start_date or not options.start_date:
        print("\n\n", parser.description, "\nSome parameters are not entered correctly."
                                          "\n\nFor more information use '--help'\n")
        sys.exit(1)

    startTime = int(datetime.strptime(options.start_date + ' 00:00:00', '%d.%m.%Y %H:%M:%S').timestamp())
    endTime = int(datetime.strptime(options.end_date + ' 23:59:59', '%d.%m.%Y %H:%M:%S').timestamp())

    if startTime > endTime:
        print(parser.description, "\nSome parameters are not entered correctly."
                                  "\n\nFor more information use '--help'\n")
        sys.exit(2)

    if not os.path.isdir(path.abspath(options.output)):
        print("Default or your directory is not exist! I create it!)")
        os.mkdir(path.abspath(options.output))

    interval = endTime - startTime
    iterations = interval // 10

    records = list()
    records.append(startTime)
    for i in range(iterations):
        records.append(records[i] + 10)
    records.append(endTime)

    for num in range(options.num_files):
        with open(path.abspath(path.join(options.output, "input_"+str(num+1))), 'w+') as f:
            for i in range(iterations):
                if random.randint(0, 100) // options.incorrect == 0:
                    f.write("Bad string, 1 \n" + str(datetime.strptime(str(datetime.fromtimestamp(records[i])),
                                                                                '%Y-%m-%d %H:%M:%S')) + "\n")
                errorType = random.randint(0, 7)
                f.write(str(datetime.fromtimestamp(records[i])) + "," + str(errorType) + "\n")


if __name__ == '__main__':
    main()