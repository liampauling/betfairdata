import os
import zipfile
from io import StringIO
import pandas as pd
import numpy as np
import pandas.io.sql as pd_sql
import pymysql
import queue
import threading
import datetime
pymysql.install_as_MySQLdb()


def create_data_frame(data, loadId):
    df = pd.read_csv(data, header=0, error_bad_lines=False)
    df['SETTLED_DATE'] = df['SETTLED_DATE'].str[:10]
    # pd.to_datetime(df['SETTLED_DATE'], format='%d-%m-%Y %H:%M:%S')
    df = df.drop(['ODDS', 'SELECTION_ID', 'WIN_FLAG', 'EVENT_ID'], axis=1)
    if 'COURSE' in df.columns:
        t = df.groupby(['SPORTS_ID', 'COUNTRY', 'COURSE', 'SETTLED_DATE', 'IN_PLAY'], sort=True).sum()
    else:
        t = df.groupby(['SPORTS_ID', 'SETTLED_DATE', 'IN_PLAY'], sort=True).sum()
    t['LOAD_ID'] = loadId
    t['VOLUME_MATCHED'] = np.round(t['VOLUME_MATCHED'], 2)
    t['DATE_LOADED'] = datetime.datetime.now()
    return t


def write_to_sql(dataframe, tablename='BETFAIR_ALL_DATA'):
    pd_sql.to_sql(dataframe, tablename, conn, if_exists='append', flavor='mysql')


def test_if(loadId):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM BETFAIR_ALL_DATA WHERE LOAD_ID = %s", (loadId,))
    output = cur.fetchone()[0]
    if output == 0:
        return True
    else:
        return False


def load_to_list():
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT(LOAD_ID) FROM BETFAIR_ALL_DATA")
    out = cur.fetchall()
    COLUMN = 0
    column = [elt[COLUMN] for elt in out]
    return column


def my(myQu):
    print('starting mysql worker')
    while True:
        df = myQu.get()
        write_to_sql(df)
        print('written')


def worker(fileQu, myQu):
    print('starting worker')
    while not fileQu.empty():
        file = fileQu.get()
        print('file', file)
        fn = file
        filename = os.path.splitext(file)[0]
        csv_file = '.'.join([os.path.splitext(fn)[0], 'csv'])

        if os.path.splitext(fn)[1] == '.zip':
            try:
                filehandle = open(file, 'rb')
                zfile = zipfile.ZipFile(filehandle)

                for f in zfile.infolist():
                    try:
                        csv_file = '.'.join([os.path.splitext(f.filename)[0], 'csv'])

                        data = StringIO(zfile.read(csv_file).decode('utf-8'))
                        df = create_data_frame(data, fn)
                        myQu.put(df)
                        # time.sleep(10000)
                    except:
                        csv_file = '.'.join([os.path.splitext(f.filename)[0], 'txt'])

                        data = StringIO(zfile.read(csv_file).decode('utf-8'))
                        df = create_data_frame(data, fn)
                        myQu.put(df)
            except:
                print()
                print('error    ', file)
                print()
        elif os.path.splitext(fn)[1] == '.csv':
            if 'rar' in fn:
                fn = os.path.join('download', str(os.path.splitext(fn)[0].split('/')[2]) + '.csv')
            else:
                fn = os.path.join('download', str(os.path.splitext(fn)[0].split('/')[1]) + '.csv')
            data = open(fn)
            df = create_data_frame(data, fn)
            myQu.put(df)
        elif os.path.splitext(fn)[1] == '.rar':
            pass
        else:
            print('error', file)


conn = pymysql.connect(host='', port=3306,
                       user='', passwd='', db='DATA')
myQu = queue.Queue()
fileQu = queue.Queue()
threading.Thread(target=my, args=(myQu,), daemon=True).start()

loads = load_to_list()
print(len(loads))
toloads = []

for subdir, dirs, files in os.walk('download'):
    files = [f for f in files if not f[0] == '.']
    for fn in files:
        if fn.split('.')[1] == 'zip':
            file = os.path.join('download', fn)
            z = os.path.join('download', fn)
        elif fn.split('.')[1] in ['csv']:
            file = os.path.join(str(os.path.splitext(fn)[0]) + '.csv')
            z = os.path.join(str(os.path.splitext(fn)[0]) + '.' + fn.split('.')[1])
        if file not in loads:
            fileQu.put(file)
            toloads.append(file)
        else:
            os.remove(z)

print('toload', len(toloads), toloads)


worker(fileQu, myQu)
