# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日
通过接口，计算前一天所有电站强度
可用作定时任务
@author: Administrator
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir
import time
from multiprocessing import Process,Manager
import datetime
import os
import sys
import traceback

os.chdir(sys.path[0])

now = datetime.datetime.today()
# sta_data_date="2019-04-18 00:00:00"
# sta_data_date=(now+ datetime.timedelta(days = -1)).strftime("%Y-%m-%d")+" 00:00:00"
# # end_data_date="2019-04-19 00:00:00"
# end_data_date=now.strftime("%Y-%m-%d")+" 00:00:00"
# sta_data_date='2019-04-01 00:00:00'
# end_data_date='2019-04-02 00:00:00'
#根据参数范围查询遍历不同的电站
def getStackInfo(que,i,sta_codes,ah_stas,sta_names,sta_data_date,end_data_date):
    global need_data
    print('***********正在计算“'+sta_names[i]+'”强度中***********')
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[i])
    rs_en=obj.getDataByBAS()[0]
#     rs_ah=[]

#     rs_ah=obj.getDataByCLS(ah_sta=ah_stas[i])[0]
#     rs_ah=obj.getDataByCLS(ah_sta=ah_stas[i],isEndAhCount=True)[0]

#     if  rs_en is not False:
#         mkdir('./strengths/'+sta_names[i])
#         write=pd.ExcelWriter(r'./strengths/'+sta_names[i]+'/'+sta_data_date.split(' ')[0]+'强度数据统计.xlsx')
#         rs_en.to_excel(write,'堆累计充放电量数据',index=False)
#         write.save()
#         print('√√√√√√√√√√√√ data is saved! √√√√√√√√√√√√')
#     print(rs_en)
    if rs_en is not False:
#     if rs_ah is not False and rs_en is not False and rs_en.ix[0,5]>0:
        #确保目录存在
        mkdir('./201906_product_files/'+sta_names[i])
        write=pd.ExcelWriter(r'./201906_product_files/'+sta_names[i]+'/'+sta_data_date.split(' ')[0]+'强度数据统计.xlsx')
#         rs_ah.to_excel(write,'簇累计充放容量数据',index=False)
        rs_en.to_excel(write,'堆累计充放电量数据',index=False)
        write.save()
        print('√√√√√√√√√√√√ data is saved! √√√√√√√√√√√√')
    else:
        print('data not need to save!')
    print(que.get())
    que.task_done()  
def multiprocess(m,n,sta_codes,ah_stas,sta_names,sta_data_date,end_data_date):
    ps=[]
#     lock = Lock()
    #通过队列中的数量,来监测是否执行完子进程
    mr = Manager()
    que=mr.Queue(5)#指定队列大小为5    
    for i in range(m,n):
        p = Process(target=getStackInfo,args=(que,i,sta_codes,ah_stas,sta_names,sta_data_date,end_data_date))
        ps.append(p)
    for i in range(len(ps)):
        while True:
            if que.full() is not True:
                que.put('进程 %s start to End'%i)
                ps[i].start()#每个进程还是会执行函数外的变量，加上本身一次，总共4次     
                break
            time.sleep(60)    
    for i in range(len(ps)):  
        ps[i].join()

if __name__ == '__main__':
    try:
        ds=pd.date_range("2018-12-26", "2019-04-22")
        for d in ds:
            sta_data_date=d.strftime("%Y-%m-%d")+' 00:00:00'
            end_data_date=d.strftime("%Y-%m-%d")+' 23:59:59'
            start=time.time()
        #     obj=getStaWork(sta_data_date, end_data_date,sta_code='0005')
        #     all_sta_codes=obj.getAllStaCode()
        #     df=pd.DataFrame(all_sta_codes)
        #     df.to_excel('./assets/sta_cl_ah_config.xlsx')
            sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
            sta_codes,sta_names,ah_stas=sta_config['code'],sta_config['name'],sta_config['capacity']
    #         multiprocess(0,1,sta_codes,ah_stas,sta_names)
    #         for i in range(0,len(sta_codes),5):
            
            multiprocess(1,2,sta_codes,ah_stas,sta_names,sta_data_date,end_data_date)
            end=time.time()
            infos="\n debug"+sta_data_date.split(' ')[0]+'查询强度数据代码运行成功！总计耗时：'+str(end-start)+"秒"
    except Exception as e:
        print(e)
#         traceback.print_exc()
        infos="\n"+sta_data_date.split(' ')[0]+"查询强度数据代码运行失败！"
    with open(r'C:\Users\Administrator\Desktop\自动运行文件日志.log', 'a') as f:
        f.write(infos) 
