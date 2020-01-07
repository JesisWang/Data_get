'''
Created on 2019年8月7日

@author: JesisW
'''
from getAPI import getStaWork
import os
import sys
import pandas as pd


if __name__ == '__main__':
#     sys.path
#     os.chdir(sys.path[0])
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    index = [0,1,2,5,6,7,14,15,16,33]  #调频电站序号
    sta_codes,sta_names=sta_config['code'],sta_config['name']
    Aim_codes,Aim_names = sta_codes[0],sta_names[0]
    a = getStaWork('2019-04-17 00:00:00','2019-04-17 01:00:00',Aim_codes)
    df1,df2 = a.getEMSAgc()