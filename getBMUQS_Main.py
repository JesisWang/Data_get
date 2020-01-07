# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日

@author: Administrator
'''
from getAPI import getStaWork
import pandas as pd
import numpy as np
from mkdir import mkdir
from multiprocessing import Process,Manager
import time

def getStackInfo(que,i,sta_codes,sta_names,sta_data_date,end_data_date,mode):
    sbox,ebox=0,-1
    sba,eba=0,-1
    scl,ecl=0,-1
    sp,ep=0,-1
    if len(mode)==1 and 'sta' in mode:
        #i就表示电站id
        pass
    #mode=[id,'box']
    elif len(mode)==2 and 'box' in mode:
        sbox,ebox=i,i+1
        i=mode[0]
    elif len(mode)==3 and 'ba' in mode:
        sba,eba=i,i+1
        i=mode[0]
        sbox,ebox=mode[1],mode[1]+1
    elif len(mode)==4 and 'cl' in mode:
        scl,ecl=i,i+1
        i=mode[0]
        sbox,ebox=mode[1],mode[1]+1
        sba,eba=mode[2],mode[2]+1
    elif len(mode)==5 and 'pack' in mode:
        sp,ep=i,i+1
        i=mode[0]
        sbox,ebox=mode[1],mode[1]+1
        sba,eba=mode[2],mode[2]+1
        scl,ecl=mode[3],mode[3]+1   
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[i])
    #6-3-3 3pack sbox=5,ebox=6,sba=2,eba=3,scl=2,ecl=3,sp=2,ep=3
    rs=obj.getCellDataByPack(sbox=sbox,ebox=ebox,sba=sba,eba=eba,scl=scl,ecl=ecl,sp=sp,ep=ep)
    if len(rs)>0:
        rs=rs[0]
        df_rs=pd.DataFrame(rs)
        df_rs=pd.concat([pd.DataFrame([[sta_data_date,end_data_date]]),df_rs])
        df_rs.columns=['描述','电压']
        mkdir('./BMUQS/太阳宫/')
        fileH='./BMUQS/太阳宫/'+sta_data_date.split(' ')[0]+sta_names[i]
        if 'sta' in mode:
            fileN=fileH+'.xlsx'
        elif 'box' in mode:
#             fileN=fileH+str(mode[0]+1)+'箱.xlsx'
            fileN=fileH+str(sbox+1)+'箱.xlsx'  
        elif 'ba' in mode:
#             fileN=fileH+str(mode[0]+1)+'箱'+str(mode[1]+1)+'堆.xlsx'
            fileN=fileH+str(mode[1]+1)+'箱'+str(sba+1)+'堆.xlsx'
        elif 'cl' in mode:
#             fileN=fileH+str(mode[0]+1)+'箱'+str(mode[1]+1)+'堆'+str(mode[2]+1)+'簇.xlsx'
            fileN=fileH+str(mode[1]+1)+'箱'+str(mode[2]+1)+'堆'+str(scl+1)+'簇.xlsx'
        elif 'pack' in mode:
#             fileN=fileH+str(mode[0]+1)+'箱'+str(mode[1]+1)+'堆'+str(mode[2]+1)+'簇'+str(mode[3]+1)+'包.xlsx'
            fileN=fileH+str(mode[1]+1)+'箱'+str(mode[2]+1)+'堆'+str(mode[3]+1)+'簇'+str(sp+1)+'包.xlsx'
        else:
            print('添加更多模式...')
        df_rs.to_excel(fileN,index=False)
        print('发现如下问题[描述，12s电压]，已存入BMUQS目录下')
    else:
        print('所选时间段内没有发现任何问题')
    print(que.get())
    que.task_done()      
def multiprocess(m,n,sta_codes,sta_names,sta_data_date,end_data_date,mode=['sta']):
    ps=[]
    #通过队列中的数量,来监测是否执行完子进程
    mr = Manager()
    que=mr.Queue(4)#指定队列大小为5
    for i in range(m,n):
        p = Process(target=getStackInfo,args=(que,i,sta_codes,sta_names,sta_data_date,end_data_date,mode))
        ps.append(p)
    for i in range(len(ps)):
        while True:
            if que.full() is not True:
                que.put('进程 %s start to End'%i)
                ps[i].start()#每个进程还是会执行函数外的变量，加上本身一次，总共4次     
                break
            time.sleep(5)  
    for i in range(len(ps)):  
        ps[i].join()

if __name__ == '__main__':
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    sta_codes,sta_names=sta_config['code'],sta_config['name']
    #海丰
    st=time.time()
    for dd in range(20,21):
#         sta_data_date="2019-07-"+str(dd)+" 00:00:00"
        sta_data_date="2019-07-07 00:00:00"
        end_data_date="2019-07-07 12:00:00"
        for b in range(0,1):#箱
            mode=[12,b,'ba']
            multiprocess(0,2,sta_codes,sta_names,sta_data_date,end_data_date,mode)
    ed=time.time()
    print("总共耗时:"+str(np.round(ed-st,4))+"秒")  
    #新丰，云河，准大
#     sta_data_date="2019-05-03 00:00:00"
#     end_data_date="2019-05-04 11:30:00"
#     multiprocess(0,4,sta_codes,sta_names,sta_data_date,end_data_date)
