from datetime import datetime, timedelta
from enum import Enum

import requests
import os
import pandas as pd


class Freq(Enum):
    EVERY15MIN = 1,
    EVERY2HR = 2,
    EVERYDAY = 3


def read_save_s3_files(bucket='wallstreetbets', file_names=None, force_continue=5,
              freq=Freq.EVERY2HR, start_yyyymmdd=20211106, start_hh=17, start_mm=0):
    print('read_save_s3_files', bucket)
    for f in file_names:
        print(f)
        if freq == Freq.EVERY2HR:
            data_temp = []
            yyyymmdd = start_yyyymmdd
            hh = start_hh
            while True:
                yyyymmdd_hh = str(yyyymmdd) + '_' + f'{hh:02d}'
                url = 'https://cse-6242-reddit.s3.amazonaws.com/' + bucket + f'/{f}{yyyymmdd_hh}.csv'
                hh = (hh + 2) % 24

                update_dt =  datetime.strptime(yyyymmdd_hh, '%Y%m%d_%H')

                if hh + 2 >= 24:
                    yyyymmdd_d = datetime.strptime(str(yyyymmdd), '%Y%m%d') + timedelta(days=1)
                    yyyymmdd = yyyymmdd_d.strftime('%Y%m%d')

                    df = pd.concat(data_temp, axis=0, ignore_index=True)

                    outdir = f'output/{bucket}/{f}'
                    if not os.path.exists(outdir):
                        os.makedirs(outdir)
                    path_to_save = f'output/{bucket}/{f}/{bucket}_{f}_{yyyymmdd}.csv'

                    if not os.path.exists(path_to_save):
                        df.to_csv(path_or_buf= path_to_save, encoding='utf-8', index=False)
                    data_temp = []
                resp = requests.get(url)
                try:
                    j = resp.json()
                    df = pd.json_normalize(j)
                    if df.shape[0]>0:
                        df['update_dt'] = update_dt
                    data_temp.append(df)
                except:
                    break
        elif freq == Freq.EVERY15MIN:
            fc = force_continue
            yyyymmdd = start_yyyymmdd
            hh = start_hh
            mm = start_mm
            while True:
                yyyymmdd_hh_mm = str(yyyymmdd) + '_' + f'{hh:02d}' + '_' + f'{mm:02d}'

                update_dt =  datetime.strptime(yyyymmdd_hh_mm, '%Y%m%d_%H_%M')
                url = 'https://cse-6242-reddit.s3.amazonaws.com/' + bucket + f'/{f}_{yyyymmdd_hh_mm}.csv'
                resp = requests.get(url)
                try:
                    df = pd.read_json(resp.json())
                    if df.shape[0]>0:
                        df['update_dt'] = update_dt
                    outdir = f'output/{bucket}/{f}'
                    if not os.path.exists(outdir):
                        os.makedirs(outdir)
                    path_to_save = f'output/{bucket}/{f}/{bucket}_{f}_{yyyymmdd_hh_mm}.csv'
                    if not os.path.exists(path_to_save):
                        df.to_csv(path_or_buf=path_to_save, encoding='utf-8', index=False)
                    fc = force_continue
                except Exception as ex:
                    print(f'Missing at {yyyymmdd_hh_mm} w/ error: {ex}')
                    fc -= 1
                    if fc < 0:
                        break
                new_mm = (mm + 15)
                mm = new_mm % 60
                if new_mm >= 60:
                    if hh + 1 >= 24:
                        yyyymmdd_d = datetime.strptime(str(yyyymmdd), '%Y%m%d') + timedelta(days=1)
                        yyyymmdd = yyyymmdd_d.strftime('%Y%m%d')
                    hh = (hh + 1) % 24

        elif freq == Freq.EVERYDAY:
            data_temp = []
            yyyymmdd = start_yyyymmdd
            while True:
                if yyyymmdd != '20211123':
                    yyyymmdd = str(yyyymmdd)
                    url = 'https://cse-6242-reddit.s3.amazonaws.com/' + bucket + f'/{f}{yyyymmdd}.csv'
                    update_dt =  datetime.strptime(yyyymmdd, '%Y%m%d')
                    resp = requests.get(url)
                    try:
                        j = resp.json()
                        df = pd.json_normalize(j)
                        if df.shape[0]>0:
                            df['update_dt'] = update_dt
                        outdir = f'output/{bucket}/{f}'
                        if not os.path.exists(outdir):
                            os.makedirs(outdir)
                        path_to_save = f'output/{bucket}/{f}/{bucket}_{f}_{yyyymmdd}.csv'
                        if not os.path.exists(path_to_save):
                            df.to_csv(path_or_buf= path_to_save, encoding='utf-8', index=False)
                        #save_to_csv(data_temp, 'w', f'../output/{bucket}_{f}_{yyyymmdd}.csv')
                        yyyymmdd = datetime.strptime(yyyymmdd, '%Y%m%d') + timedelta(days=1)
                        yyyymmdd = yyyymmdd.strftime('%Y%m%d')
                    except:
                        break
                else:
                    yyyymmdd = datetime.strptime(yyyymmdd, '%Y%m%d') + timedelta(days=1)
                    yyyymmdd = yyyymmdd.strftime('%Y%m%d')


def get_all_files():
    read_save_s3_files(file_names=['author_data', 'comment_data', 'submission_data', 'text_data'],
                       bucket='wallstreetbets',
                       start_yyyymmdd=20211106)
    read_save_s3_files(file_names=['trend_data'],
                       bucket='googletrends',
                       start_yyyymmdd=20211114, freq=Freq.EVERY15MIN, force_continue=8, start_hh=6, start_mm=0)
    read_save_s3_files(file_names=['stock_data'],
                       bucket='nasdaq',
                       start_yyyymmdd=20211107, freq=Freq.EVERYDAY)
    read_save_s3_files(file_names=['author_data', 'comment_data', 'submission_data', 'text_data'],
                       bucket='investing',
                       start_yyyymmdd=20211110, start_hh=8)
    read_save_s3_files(file_names=['author_data', 'comment_data', 'submission_data', 'text_data'],
                       bucket='stocks',
                       start_yyyymmdd=20211110, start_hh=8)


if __name__ == '__main__':
    get_all_files()
