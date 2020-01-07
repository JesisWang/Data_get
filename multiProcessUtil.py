# -*- coding: utf-8 -*-
'''
Created on 2019年5月24日
多进程队列控制函数
利用队列思想监测未执行完的进程数量，控制进程数开启过多造成接口获取异常
params:
1.功能函数名
2.开始进程m,n 总数量n-m个
3.一次最多开启进程数，Pnum=5
4.超过五个以后，会每隔sl=60秒，判断一次进程是否有运行结束
5.其余参数，其内容为输入函数除了第1/2个参数外的其余所有参数
@author: thankusun
'''
import time
from multiprocessing import Process,Manager

#    sta_codes,ah_stas,sta_names=args
def multiprocess(getStackInfo,m,n,Pnum=5,sl=60,*args):
    ps=[]
#     lock = Lock()
    #通过队列中的数量,来监测是否执行完子进程
    mr = Manager()
    que=mr.Queue(Pnum)#指定队列大小为5 
    share_dict=mr.dict()#用来将结果汇总
    for i in range(m,n):
        p = Process(target=getStackInfo,args=(que,i,share_dict)+ args )
        ps.append(p)
    for i in range(len(ps)):
        while True:
            if que.full() is not True:
                que.put('进程 %s start to End'%i)
                ps[i].start()#每个进程还是会执行函数外的变量，加上本身一次，总共4次     
                break
            time.sleep(sl)       
    for i in range(len(ps)):  
        ps[i].join()
    share_dict=sorted(share_dict.items(), key=lambda x:x[0])
    return share_dict