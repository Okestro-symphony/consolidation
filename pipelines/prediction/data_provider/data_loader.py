import os
import numpy as np
import pandas as pd

import torch
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler

import warnings

warnings.filterwarnings('ignore')


class Dataset_Linear(Dataset):
    def __init__(self, root_path: str, data_path:str, metric: str, host: str,
                target: str, flag: str='train', size: list=None,
                scale: bool=False, freq: str='5min', interval: str='m'
                 ):

        if size == None:
            self.seq_len = 1 * 24 * 12
            self.label_len = 1 * 3 * 12
            self.pred_len = 1 * 3 * 12
        else:
            self.seq_len = size[0]
            self.label_len = size[1]
            self.pred_len = size[2]
        assert flag in ['train', 'test', 'val']
        type_map = {'train': 0, 'val': 1, 'test': 2, 'product': 4}
        self.set_type = type_map[flag]

        self.target = target
        self.scale = scale
        self.freq = freq
        self.interval = interval

        self.root_path = root_path
        self.data_path = data_path
        self.metric = metric
        self.host = host
        self.__read_data__()

    def __read_data__(self):
        self.file_name = os.path.join(self.data_path, self.metric.split("-")[0]+'.csv')

        df_raw = pd.read_csv(self.file_name)
        df_raw = df_raw[df_raw['host_name']==self.host]
        df_raw = df_raw[['datetime', self.target]]
        data = df_raw[[self.target]]

        if self.scale:
            self.scaler = StandardScaler()
            self.scaler.fit(data.values)
            data = self.scaler.transform(data.values)
        else:
            data = data.values


        df_stamp = df_raw[['datetime']]
        df_stamp['datetime'] = pd.to_datetime(df_stamp.datetime)


        self.data_x = data
        self.data_y = data 
        self.data_stamp = df_stamp

    def __getitem__(self, index):
        s_begin = index
        s_end = s_begin + self.seq_len
        r_begin = s_end - self.label_len
        r_end = r_begin + self.label_len + self.pred_len

        seq_x = self.data_x[s_begin:s_end]
        seq_y = self.data_y[r_begin:r_end]

        return seq_x, seq_y

    def __len__(self):
        return len(self.data_x) - self.seq_len - self.pred_len + 1

    def inverse_transform(self, data):
        return self.scaler.inverse_transform(data)
        


class Dataset_Stat_models(Dataset):
    pass


class Dataset_Pred(Dataset):
    pass