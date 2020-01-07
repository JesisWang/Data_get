# -*- coding: utf-8 -*-
'''
Created on 2019年5月10日
@author: Administrator
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
import traceback
import time
from multiprocessing import Process,Manager
import datetime
import os
import sys

mpl.rcParams['font.sans-serif']=['SimHei'] #指定默认字体 SimHei为黑体
mpl.rcParams['axes.unicode_minus']=False #用来正常显示负号

os.chdir(sys.path[0])

now = datetime.datetime.today()
sta_data_date=(now+ datetime.timedelta(days = -1)).strftime("%Y-%m-%d")+" 00:00:00"
#"2019-04-20 00:00:00"
end_data_date=now.strftime("%Y-%m-%d")+" 00:00:00"
# end_data_date=(now+ datetime.timedelta(days = -1)).strftime("%Y-%m-%d")+" 00:00:00"
sta_data_date="2019-07-20 00:00:00"
end_data_date="2019-07-21 00:00:00"

#根据参数范围查询遍历不同的电站
def getStackInfo(que,sta_i,sta_codes,sta_names):
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[sta_i])
    mkdir('./temp_datas/'+sta_names[sta_i])
    #默认 0 -1
    sbox,ebox=0,-1
    sba,eba=0,-1
    scl,ecl=0,-1
    sp,ep=0,-1
    #默认以包，簇，堆
    mode=['']
    
    rs=obj.getCellDataByPack(quickly=False,sbox=sbox,ebox=ebox,sba=sba,eba=eba,scl=scl,ecl=ecl,sp=sp,ep=ep,mode=mode)
#     rs=pd.read_excel('D:\\python_works\\ClouPy\works\\API_Class\\temp_datas\\河北宣化电厂储能电站\\2019-05-26.xlsx',sheet_name=0,index_col=0)
#     rs=np.array(rs)
#     print(rs)
    pack_bmu_num=obj.pack_bmu_num
    if ebox==-1:
        ebox=obj.box_num
    if ep==-1:
        ep=obj.pack_num

#     rs=[[[25.0, 23.0, 2.0, 24.333333333333332], [25.0, 23.0, 1.0, 24.166666666666668], [26.0, 23.0, 1.0, 25.166666666666668], [25.0, 23.0, 1.0, 24.166666666666668], [25.0, 23.0, 1.0, 24.833333333333332], [25.0, 24.0, 1.0, 25.0], [25.0, 23.0, 1.0, 24.666666666666668], [25.0, 23.0, 2.0, 24.666666666666668], [25.0, 23.0, 1.0, 24.666666666666668], [25.0, 23.0, 1.0, 24.666666666666668], [25.0, 23.0, 1.0, 24.666666666666668], [25.0, 23.0, 1.0, 24.833333333333332], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN]], [[26.0, 23.0, 2.0, 24.555555555555557], [26.0, 23.0, 2.0, 24.62962962962963], [25.0, 23.0, 2.0, 24.555555555555557], [25.0, 23.0, 2.0, 24.77777777777778], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN],[np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN], [np.NaN,np.NaN,np.NaN,np.NaN]], [[26.0, 23.0, 2.0, 24.970807875084862], [np.NaN,np.NaN,np.NaN,np.NaN]]]

    print([eba,ep,pack_bmu_num])
    print(rs)
#     obj.cl_num_list=[4,5]
    print(obj.cl_num_list)
    if len(rs)==len(mode):
        write=pd.ExcelWriter("./temp_datas/"+sta_names[sta_i]+'/'+sta_data_date.split(' ')[0]+".xlsx")
        for i in range(len(rs)):
            #对数据格式化处理，便于画图
            formatRs,ys,xs,fxs=[],[],[],[]
            for box in range(sbox+1,ebox+1):
                boxformatRs=[]#每一箱
                Flag_ba=False
                if eba==-1:
                    eba=obj.ba_num_list[box-1-sbox]
                    print('%s-------:'%eba)
                    Flag_ba=True
                for ba in range(sba+1,eba+1):
                    xba=str(ba)+'堆'
                    A=sum(obj.ba_num_list[0:box-1-sbox])#(box-sbox-1)*(eba-sba)#已经循环过的堆数
                    if mode[i]=='getDTempsByBA':
                        try:
                            boxformatRs.append(rs[i][(ba-sba-1)+A])
                            fxs.append(str(box)+'箱-'+xba)
                        except:
                            boxformatRs.append([np.NaN,np.NaN,np.NaN,np.NaN])                            
                        if box==sbox+1:
                            xs.append(xba)
                    else:
                        
                        if ecl==-1:
                            ecl=obj.cl_num_list[ba-1-sba+A]#(box-1-sbox)*(eba-sba)
                            print(ecl)
                            FLag=True
                        for cl in range(scl+1,ecl+1):
                            xcl=xba+'-'+str(cl)
#                             B=(A+(ba-sba-1))*(ecl-scl)
                            B=sum(obj.cl_num_list[0:ba-1-sba+A])#(box-1-sbox)*(eba-sba)
                            if mode[i]=='getDTempsByCL':
                                try:
                                    boxformatRs.append(rs[i][(cl-scl-1)+B])
                                    fxs.append(str(box)+'箱-'+xcl)
                                except:
                                    boxformatRs.append([np.NaN,np.NaN,np.NaN,np.NaN])                                    
                                if box==sbox+1:
                                    xs.append(xcl)
                            else:
                                for p in range(sp+1,ep+1):                                    
                                    xp=xcl+'-Pack'+str(p)
                                    C=(B+(cl-scl-1))*(ep-sp)
                                    if mode[i]=='getDTempsByPack':
                                        try:
                                            fxs.append(str(box)+'箱-'+xp)                                   
                                            boxformatRs.append(rs[i][(p-sp-1)+C])
                                        except:
                                            boxformatRs.append([np.NaN,np.NaN,np.NaN,np.NaN])                                            
                                        if box==sbox+1:
                                            xs.append(xp)
                                    else:
                                        for bmu in range(1,pack_bmu_num+1):
                                            xb=xp+'-BMU'+str(bmu)
                                            D=(C+(p-sp-1))*pack_bmu_num
                                            if 'getDTempsByBMU' in mode:
                                                try:
                                                    fxs.append(str(box)+'箱-'+xb)
                                                    boxformatRs.append(rs[i][(bmu-1)+D])
                                                except:
                                                    boxformatRs.append([np.NaN,np.NaN,np.NaN,np.NaN])                                                    
                                                if box==sbox+1:
                                                    xs.append(xb)
                        if FLag:
                            ecl=-1
                if Flag_ba:
                    eba=-1    
                formatRs.append(boxformatRs)
                ys.append('第%s箱'%box)
            
            if len(xs)>0:
                xs_split=xs[0].split('-')
                if len(xs_split)==1:
                    xlabel='堆'
                    txt='各堆'
                elif len(xs_split)==2:
                    xlabel='堆-簇'
                    txt='各簇'
                elif len(xs_split)==3:
                    xlabel='堆-簇-包'
                    txt='各包'
                elif len(xs_split)==4:
                    xlabel='堆-簇-包-BMU'
                    txt='各BMU'
            #保留原格式数据到表格
            df_r=pd.DataFrame(rs[i])
            print(df_r)
            df_r.columns=['最高温度/℃','最低温度/℃','最大温差/℃','最大平均温度/℃']
            print(fxs)
            df_r.index=fxs
            df_r.to_excel(write,sta_names[sta_i]+txt+"温度数据",index=True)
            #画图最高温度与最大温差，最大平均与最低温度不画
            maxTs,max_dif_Ts=[],[]

            for it_box in formatRs:
                maxTs.append([it[0] for it in it_box])
                max_dif_Ts.append([it[2] for it in it_box])
            df = pd.DataFrame(maxTs)
#             print(df)
            df.index=ys
            df.columns=xs
            
            f,(ax1,ax2) = plt.subplots(figsize = (10, 8),nrows=2)
            cmap = sns.cubehelix_palette(start = 1.5, rot = 3, gamma=0.8, as_cmap = True)
            try:
                sns.heatmap(df, linewidths = 0.05, ax = ax1,annot=True, annot_kws={'weight':'bold'},vmax=max(np.max(df)), vmin=min(np.min(df)), cmap=cmap)
                ax1.set_title(sta_data_date.split(' ')[0]+sta_names[sta_i]+txt+'最高温度')
                ax1.set_xlabel('')
                ax1.set_xticklabels([]) #设置x轴图例为空值
                ax1.set_ylabel('箱')
            except:
                print('图像绘制失败')
            df = pd.DataFrame(max_dif_Ts)
            df.index=ys
            df.columns=xs
            try:
                sns.heatmap(df,ax = ax2,cmap=sns.color_palette("Reds", 7),cbar=False,annot=True,mask=df <max(np.max(df)), annot_kws={"weight": "bold",'size':25}) 
                sns.heatmap(df,linewidths = 0.05,vmax=max(np.max(df)),vmin=min(np.min(df)), cmap=sns.color_palette("Reds", 7),ax = ax2,annot=True,mask=df >=max(np.max(df)))
                # rainbow为 matplotlib 的colormap名称
                ax2.set_title(sta_data_date.split(' ')[0]+sta_names[sta_i]+txt+'最大温差')
                ax2.set_xlabel(xlabel)
                ax2.set_ylabel('箱')
                plt.savefig("./temp_datas/"+sta_names[sta_i]+'/'+sta_data_date.split(' ')[0]+txt+'最高温度与最大温差.png')
            except:
                print('图像绘制失败')
        write.save()
    print(que.get())
    que.task_done()     
#rge=[1,2,3]        
def multiprocess(rge,sta_codes,sta_names):
    ps=[]
    #     lock = Lock()
    #通过队列中的数量,来监测是否执行完子进程
    mr = Manager()
    que=mr.Queue(5)#指定队列大小为5   
    for i in rge:
        p = Process(target=getStackInfo,args=(que,i,sta_codes,sta_names))
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
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    sta_codes,sta_names=sta_config['code'],sta_config['name']
    print(sta_config['name'])
    ins=input('请输入编号(如新丰电站编号为0,多个使用英文逗号隔开)：')
    ns=ins.split(',')
    runFlag=True
    for item in ns:
        try:
            if int(item)>=len(sta_codes):
                print('输入编号不正确：例如：0,1 请打开重试！')
                runFlag=False
                break
        except:
            print('输入编号不正确：例如：0,1 请打开重试！')
            runFlag=False
            break
    if runFlag:
        print('-----开始处理数据中----默认以包、簇、堆分组将分析结果返回-----')
        ns=list(map(int,ns))
        infos='debug...'
        try:
            multiprocess(ns[0:len(ns)],sta_codes,sta_names)
            infos=infos+sta_data_date.split(' ')[0]+"温度运行成功！"+"\n"
        except:
            infos=infos+sta_data_date.split(' ')[0]+"查询电站编码"+str(ns[i])+"温度运行失败！"+"\n"
        with open(r'C:\Users\Administrator\Desktop\自动运行文件日志.log', 'a') as f:
            f.write(infos) 
