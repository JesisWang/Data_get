# -*- coding: utf-8 -*-
'''
Created on 2019年5月24日
调用多进程函数使用事例
@author: thankusun
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir
import multiProcessUtil as ml
import datetime
import os
import sys


os.chdir(sys.path[0])

now = datetime.datetime.today()
# sta_data_date="2019-04-18 00:00:00"
sta_data_date=(now+ datetime.timedelta(days = -1)).strftime("%Y-%m-%d")+" 00:00:00"
# end_data_date="2019-04-19 00:00:00"
end_data_date=now.strftime("%Y-%m-%d")+" 00:00:00"

#功能函数
def getStackInfo(que,i,needRs,sta_codes,ah_stas,sta_names):
    print('***********正在计算“'+sta_names[i]+'”强度中***********')
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[i])
    rs_en=obj.getDataByBAS()[0]
    rs_ah=obj.getDataByCLS(ah_sta=ah_stas[i])[0]
#    rs_ah=obj.getDataByCLS(ah_sta=ah_stas[i],isEndAhCount=True)[0]
#    return 
#     print(rs_en)
    if rs_ah is not False and rs_en is not False and rs_en.ix[0,5]>0:
        #确保目录存在
        mkdir('./strengths/'+sta_names[i])
        write=pd.ExcelWriter(r'./strengths/'+sta_names[i]+'/'+sta_data_date.split(' ')[0]+'强度数据统计.xlsx')
        rs_ah.to_excel(write,'簇累计充放容量数据',index=False)
        rs_en.to_excel(write,'堆累计充放电量数据',index=False)
        write.save()
        print('√√√√√√√√√√√√ data is saved! √√√√√√√√√√√√')
    else:
        print('data not need to save!')
    print(que.get())
    que.task_done()
    
if __name__ == '__main__':
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    sta_codes,sta_names,ah_stas=sta_config['code'],sta_config['name'],sta_config['capacity']
    ml.multiprocess(getStackInfo,0,6,5,60,sta_codes,ah_stas,sta_names)