# -*- coding: utf-8 -*-
'''
WebService接口使用
Created on 2019年5月9日
函数功能说明：
1)comvs:供类内部调用，辅助用于BMU问题判断
2)getDTs:供类内部调用，返回时间段内的时间序列
3)getClusterCodes:供类内部调用，返回所有簇编码
4)getBMUDataByCls:供类外部调用，100011接口(按簇取，不需要bmu编号)，获取各个簇详细数据，以bmu为最小单元
5)getDataByBAS:供类外部调用，100013接口，根据mode获取堆的某些数据
  mode=[强度，时间，累计充电量，累计放电量，电压，电流，功率，soc]
  mode=['getStrengths','getDateTime','getChargeEns','getDisChargeEns','getVols','getCurs','getPws','getSocs']
6)getDataByCLS:供类外调用，100004，单元为簇，
  mode=[强度，时间，累计充电安时，累计放电安时,电压，电流，Soc,告警状态字，保护状态字，Bmu数量]
  mode=['getStrenths','getDateTime','getChargeAh','getDisChargeAh','getVols','getCurs','getSocs','getWarns','getProts']
7)getPCSData:供类外部调用，  200002接口，获取PCS数据，频率，有功功率，直流电压、电流，直流功率
  return [{'data_date':data_date,'p_udc':all_p_udc,'i_udc':all_i_udc,'u_udc':all_u_udc,'pz':all_pz,'p_rate':all_p_rate}]
8)getAllStaCode:供外部类调用，获取目前所有电站名及编码
9)getBMUQS:供类内部调用，BMU问题判别算法
10)getCellDataByPack:供类外部调用，100006,quickly==False,存储详细电压温度数据至文档，
   mode=对BMU进行问题分析，分别以BMU，包、簇、堆为单位进行温度统计
   mode=['getBMUQS','getDTempsByBMU','getDTempsByPack','getDTempsByCL','getDTempsBA'] 
11)getEventDataByBAS:供类外部调用，100009，事件数据分析----更新中
12)getEMSAgc:供类外调用，200005,200006,AGC数据获取，
    return = 1#机组  [AGC,机组功率];2# [机组AGC,机组功率]
13)getEMSBat:供类外调用，200007,200008,储能系统功率获取(EMS采集),
    return = 系统1,2同时间数据;系统1数据;系统2数据
@author: 逻辑的使命,wby
'''
import suds
import time
import datetime
import json
import numpy as np
from suds.client import Client
import pandas as pd
import os
import sys

os.chdir(sys.path[0])

sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
sta_codes,sta_names,ah_stas=sta_config['code'],sta_config['name'],sta_config['capacity']
# 更新电站：M = getStaWork(start,end,'0010').getAllStaCode()
#        for i in M:
#            if i[1] in sta_config['name']:
#                print('已存在')
#            else:
#                sta_config.loc[len(org),0:2]=list(tuple(i))
class getStaWork(object):
    '''
        webservice接口函数使用类大全
    '''
    sta_data_date="2019-05-04 00:00:00"
    end_data_date="2019-05-04 15:00:00"
    sta_code="0010"#电站编号：海丰
    ratio=0.9#计算等效循环时系数
    #接口编码，分别对应电池堆集合档案、电池堆编码结构、电池簇数据统计、电池簇综合曲线簇、电芯数据统计、电池簇综合曲线电芯接口,簇数据，堆数据
    methods=["100001","100002",'100003',"100004",'100005','100006','100009','100011','100013','200003','200004','200005','200006']
    #箱子总数量，堆编码列表,每个模组中有多少Bmu,每个簇中有多少模组,每个Bmu中有多少cell
    box_num,bmsCodes,pack_bmu_num,pack_num,cell_num,ba_num1=0,0,0,0,0,0
    cl_num_list=[]
    ba_num_list=[]
    ### 
    def comvs(self,*agrs):
        if len(agrs)>=2:
            flag=True
            for i in range(0,len(agrs)-1):
                if abs(agrs[i]-agrs[i+1])<=0.003:
    #                 print([agrs[i],agrs[i+1]])
                    continue
                else:
                    flag=False
                    break
            return flag
        else:
            print('输出参数至少为两个')
    #根据条数，划分时间得序列，有些最大支持200，有些支持5000
    #ds=[]#存放时间范围，xx-xx-xx 00:00:00,xx-xx-xx 12:00:00
    def getDTs(self,max_num=5000):
        if max_num==5000:
            freq='12H'
        elif max_num==200:
            freq='0.5H'
        else:
            freq='1H'
        ds=pd.date_range(self.sta_data_date, self.end_data_date, freq=freq).astype('str')
        if len(ds)==1:
#             help(ds)
#             ds.iloc[0]=self.sta_data_date
            ds=ds.delete(0)
            ds=ds.insert(0, self.sta_data_date)
        if ds[-1]!=self.end_data_date:
            ds=ds.insert(len(ds), self.end_data_date)
        return ds
    #100002 获取某箱某堆的所有簇编码
    def getClusterCodes(self,box,ba):
        bmsCode=self.bmsCodes[box][ba]
        strJson=str({'bms_code':bmsCode})
        clusterJson=json.loads(self.client.service.getDoc(self.code,self.methods[1],strJson))
        try:
            #注有些电站返回不合理，没有相关字段，比如天长市美好乡村智能微电网
            #查询一个包中有多少个bmu,同一电站的包bmu数量是一样的，故只取一个即可
            self.pack_bmu_num=clusterJson["data"][0]['clusters_data'][0]['packs_data'][0]['pack_bmu_num']
            self.pack_num=clusterJson["data"][0]['clusters_data'][0]['pack_num']
            self.cell_num=clusterJson["data"][0]['clusters_data'][0]['packs_data'][0]['bmus_data'][0]['cell_num']
            clusterCodes=[it['cluster_code'] for it in clusterJson['data'][0]['clusters_data']]
            return clusterCodes
        except:
            return False
#         for i in range(len(ds)-1):
#             print([ds[i],ds[i+1]])
    #使用100011接口(按簇取，不需要bmu编号)，获取各个簇详细数据，以bmu为最小单元，获取时间段内所有数据
    def getBMUDataByClS(self,sbox=0,ebox=-1,sba=0,eba=-1,scl=0,ecl=-1,mode='getTemps',bum_num=18):
        pageSize=200
        ds=self.getDTs(pageSize)
        allrs=[]
        returnobj=[]
        if ebox==-1:
            rgbox=range(sbox,self.box_num)
        else:
            rgbox=range(sbox,ebox)
        for box in rgbox:
            if eba==-1:
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                clusterCodes=self.getClusterCodes(box,ba)
                if clusterCodes is False:
                    return False
                if ecl!=-1 and ecl>scl>=0:
                    clusterCodes=clusterCodes[scl:ecl]
                for cluster_code in clusterCodes:
                    rs,rs1,rs2,rs3,rs4=[],[],[],[],[]
                    for i in range(len(ds)-1):
                        sta_data_date=ds[i]
                        end_data_date=ds[i+1]
                        dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "cluster_code": cluster_code, 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                        }
                        try:
                            result=json.loads(self.client.service.getData(self.code,self.methods[7],str(dataJson)))
                        except:
                            time.sleep(6)
                            result=json.loads(self.client.service.getData(self.code,self.methods[7],str(dataJson)))
                        if(result['jszt']=='0' and len(result['data'])>0):
                            #注意按bmu进行排序!!!
                            for cl_ord in range(len(result['data'])):
                                try:
                                    result['data'][cl_ord]['clusterdatas'].sort(key=lambda x:x['package_data_order'])
                                    if bum_num==0:
                                        bum_num=len(result['data'][cl_ord]['clusterdatas'])
                                except:
                                    break
                            if 'getTemps' in mode:
                                temp_data=[pack['temp_data'] for cl in result['data'] for pack in cl['clusterdatas']]
                                for i in range(0,len(temp_data),bum_num):#18bmu,19bmu
                                    rs.append(temp_data[i:i+bum_num])
                            elif 'getVols&Is' in mode:
                                cell_v_data=[pack['cell_v_data'] for cl in result['data'] for pack in cl['clusterdatas']]
                                   
                                #每个bmu的电流是一样的，都是簇电流数据，故只取一个即可
                                curs_data,dates_date,socs_data=[],[],[]
                                for nnnn in range(len(result['data'])):
                                    try:
                                        curs_data.append(result['data'][nnnn]['clusterdatas'][0]['current'])
                                        if 'SOCs' in mode:#此SOC与蔟不一样，目前值无效都是0 2019-12
                                            socs_data.append(result['data'][nnnn]['clusterdatas'][0]['soc'])
#                                     curs_data=[cl['clusterdatas'][0]['current'] for cl in result['data']]
                                       #如果还需要时间，则获取同时保留时间
                                        if 'Time' in mode:
                                            dates_date.append(result['data'][nnnn]['data_date'])
                                    except:
                                        print(result['data'][nnnn])
                                        continue
                                for i in range(0,len(cell_v_data),bum_num):#18bmu,19bmu
                                    rs.append(cell_v_data[i:i+bum_num])
                                rs1=rs1+curs_data
                                if 'SOCs' in mode:
                                    rs4=rs4+socs_data
                                if 'Time' in mode:
                                    rs2=rs2+dates_date
                                if 'Temps' in mode:
                                    temp_data=[pack['temp_data'] for cl in result['data'] for pack in cl['clusterdatas']]
                                    for i in range(0,len(temp_data),bum_num):#18bmu,19bmu
                                        rs3.append(temp_data[i:i+bum_num])
                    df_rs=pd.DataFrame(rs)
                    if 'getTemps' in mode:
                        if mode=='getTemps_NO_Save':
                            pass
                        else:   
                            df_rs.to_csv('%s温度数据.csv'%cluster_code)
                        allrs.append(rs)
                    if mode=='getVols&Is':
                        df_rs1=pd.DataFrame(rs1)
                        allrs.append([df_rs,df_rs1])
                    if 'getVols&Is&Time' in mode:
                        df_rs1=pd.DataFrame(rs1)
                        df_rs2=pd.DataFrame(rs2)
                        #soc与temp 不一起返回
                        if 'SOCs' in mode:
                            df_rs4=pd.DataFrame(rs4)
                            allrs.append([df_rs,df_rs1,df_rs2,df_rs4])
                        elif 'Temps' in mode:#如果还需要温度，则一起返回
                            df_rs3=pd.DataFrame(rs3)
                            allrs.append([df_rs,df_rs1,df_rs2,df_rs3])
                        else:
                            allrs.append([df_rs,df_rs1,df_rs2])#电压，电流，时间
                    print('计算完第%s'%cluster_code)
                    returnobj.append(cluster_code)
        if 'getVols&Is' in mode:
            return allrs,returnobj
        else:
            return allrs
    #获取堆的数据100013接口,某箱某堆,quickly快捷版不需要任何中间数据
    def getDataByBAS(self,sbox=0,ebox=-1,sba=0,eba=-1,quickly=True,mode=['getStrenths']):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_c_ens=[]#累计充电量kWh
        all_d_ens=[]#累计放电量kWh
        vols=[]#堆电压
        curs=[]#堆电流
        pws=[]#堆总功率
        socs=[]#堆soc
        data_date=[]
        rs=[]#返回计算后的结果
        Flag=False#记录是否查询所有
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                print('-------正在处理第'+str(box+1)+"箱第"+str(ba+1)+"堆的数据----------")
                temp_c_ens,temp_d_ens,temp_vols,temp_curs,temp_pws,temp_socs,temp_data_date=[],[],[],[],[],[],[]
                j=0
                if quickly:
                    rge=[0,len(ds)-2]
                else:
                    rge=range(len(ds)-1)
                for i in rge:
                    sta_data_date=ds[i]
                    end_data_date=ds[i+1]
                    print("处理"+sta_data_date)
                    dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                    }
#                     print(dataJson)
                    try:
                        result=json.loads(self.client.service.getData(self.code,self.methods[8],str(dataJson)))
                    except:
                        time.sleep(10)
                        result=json.loads(self.client.service.getData(self.code,self.methods[8],str(dataJson)))
    #               print(result)
                    #完全版，False
                    if quickly==False:
                        if(result['jszt']=='0' and len(result['data'])>0):
                            temp_c_ens=np.concatenate((temp_c_ens,[it['all_inenergy'] for it in result['data']]))
                            temp_d_ens=np.concatenate((temp_d_ens,[it['all_outenergy'] for it in result['data']]))
                            temp_vols=np.concatenate((temp_vols,[it['voltage'] for it in result['data']]))
                            temp_curs=np.concatenate((temp_curs,[it['current'] for it in result['data']]))
                            temp_pws=np.concatenate((temp_pws,[it['power'] for it in result['data']]))
                            temp_socs=np.concatenate((temp_pws,[it['soc'] for it in result['data']]))
                            temp_data_date=np.concatenate((temp_data_date,[it['data_date'] for it in result['data']]))
                    #快捷版,只处理最开始第一个与结束最后一个
                    else:
#                         print(result)
                        if(result['jszt']=='0' and len(result['data'])>0):
                            temp_c_ens.append([it['all_inenergy'] for it in result['data']][j])
                            temp_d_ens.append([it['all_outenergy'] for it in result['data']][j])
                        else:
                            temp_c_ens.append(0)
                            temp_d_ens.append(0)
                        j=j-1
                if quickly==False:
                    all_c_ens.append(temp_c_ens)
                    all_d_ens.append(temp_d_ens)
                    vols.append(temp_vols)
                    curs.append(temp_curs)
                    pws.append(temp_pws)
                    socs.append(temp_socs)
                    data_date.append(temp_data_date)
    #                 print(all_c_ens)
                if len(temp_d_ens)>1 and len(temp_c_ens)>1:
                    #快捷版可能存在两段时间内没有数据，从而进入这里
                    if temp_c_ens[0]==0:
                        print('注：开始时间段内数据缺失，计算暂用0代替') 
                    elif temp_c_ens[-1]==0:
                        print('注：截止时间段内数据缺失，计算暂用0代替')                       
                    #需要计算其累计电量的差值
                    print(['截止累计充放电量',[temp_c_ens[-1],temp_d_ens[-1]]])
                    dc=temp_c_ens[-1]-temp_c_ens[0]
                    dd=temp_d_ens[-1]-temp_d_ens[0]
                    if temp_c_ens[-1]!=0:
                        rs.append([dc,dd,temp_c_ens[-1],temp_d_ens[-1],temp_d_ens[-1]/temp_c_ens[-1]])
                    else:
                        rs.append([dc,dd,temp_c_ens[-1],temp_d_ens[-1],0])
                else:
                    print('选择时间范围内数据完全缺失，请重试！')
            if Flag:
                #如果是查询所有则恢复设置成-1
                eba=-1
        if len(rs)==0:
            return False
        return_rs=[]#最终返回结果
        if 'getStrenths' in mode:
            df_rs=pd.DataFrame(rs)
#             print(df_rs.iloc[:,2])
            sum_c_en,sum_d_en=df_rs.iloc[:,0:2].sum()
            df_rs=pd.concat([df_rs,pd.DataFrame([sum_d_en])],axis=1)
            df_rs.columns=['各堆累计充电量/kWh','各堆累计放电量/kWh','截止累计充电量/kWh','截止累计放电量/kWh','进出总效率','总计累计放电量/kWh']
            return_rs.append(df_rs)
    #         print(df_rs)
#             return df_rs
        if 'getDateTime' in mode:
            return_rs.append(data_date)
        if 'getChargeEns' in mode:
            return_rs.append(all_c_ens)
        if 'getDisChargeEns' in mode:
            return_rs.append(all_d_ens)
        if 'getVols' in mode:
            return_rs.append(vols)
        if 'getCurs' in mode:
            return_rs.append(curs)
        if 'getPws' in mode:
            return_rs.append(pws)
        if 'getSocs' in mode:
            return_rs.append(socs)      
        return return_rs
    #获取簇数据，接口100004，单元为簇
    def getDataByCLS(self,ah_sta=120,sbox=0,ebox=-1,sba=0,eba=-1,scl=0,ecl=-1,quickly=True,mode=['getStrenths'],isEndAhCount=False):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_c_ah,all_d_ah,all_vols,all_curs,all_socs,all_warn,all_prot=[],[],[],[],[],[],[]
        data_date=[]
        end_ah_count=[]#截至充分容量计数
        rs=[]#返回最后结果
        Flag=False
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                clusterCodes=self.getClusterCodes(box,ba)
                if clusterCodes is False:
                    return False
                if ecl!=-1 and ecl>scl>=0:
                    clusterCodes=clusterCodes[scl:ecl]
                for cluster_code in clusterCodes:
                    print('-------正在处理第'+cluster_code+"的数据----------")
                    #累计充电安时，放电安时，簇电压，簇电流，簇soc,告警状态字，保护状态字
                    temp_c_ah,temp_d_ah,temp_vols,temp_curs,temp_socs,temp_warn,temp_prot=[],[],[],[],[],[],[]
                    temp_data_date=[]
                    j=0
                    if quickly:
                        rge=[0,len(ds)-2]
                    else:
                        rge=range(len(ds)-1)

                    for i in rge:
                        sta_data_date=ds[i]
                        end_data_date=ds[i+1]
                        print("处理中"+sta_data_date)
                        dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "cluster_code": cluster_code, 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                        }
                        try:
                            result=json.loads(self.client.service.getData(self.code,self.methods[3],str(dataJson)))
                        except:
                            time.sleep(10)
                            try:
                                result=json.loads(self.client.service.getData(self.code,self.methods[3],str(dataJson)))
                            except:
                                result={'jszt':'-1'}
#                         print(result)                                            
                        #完全版，False
                        if quickly==False:
                            if(result['jszt']=='0' and len(result['data'])>0):
                                temp_c_ah=np.concatenate((temp_c_ah,[it['charge_ah'] for it in result['data']]))
                                temp_d_ah=np.concatenate((temp_d_ah,[it['discharge_ah'] for it in result['data']]))
                                temp_vols=np.concatenate((temp_vols,[it['voltage'] for it in result['data']]))
                                temp_curs=np.concatenate((temp_curs,[it['current'] for it in result['data']]))
                                temp_socs=np.concatenate((temp_socs,[it['soc'] for it in result['data']]))
                                temp_warn=np.concatenate((temp_warn,[it['warn_st'] for it in result['data']]))
                                temp_prot=np.concatenate((temp_prot,[it['prot_st'] for it in result['data']]))
                                temp_data_date=np.concatenate((temp_data_date,[it['data_date'] for it in result['data']]))
                        #快捷版,只处理最开始第一个与结束最后一个
                        else:
                            if(result['jszt']=='0' and len(result['data'])>0):
                                temp_c_ah.append([it['charge_ah'] for it in result['data']][j])
                                temp_d_ah.append([it['discharge_ah'] for it in result['data']][j])
                            else:
                                temp_c_ah.append(0)
                                temp_d_ah.append(0)
                            j=j-1
                    if quickly==False:
                        all_c_ah.append(temp_c_ah)
                        all_d_ah.append(temp_d_ah)
                        all_vols.append(temp_vols)
                        all_curs.append(temp_curs)
                        all_warn.append(temp_warn)
                        all_prot.append(temp_prot)
                        all_socs.append(temp_socs)
                        data_date.append(temp_data_date)       
                    if len(temp_d_ah)>1 and len(temp_c_ah)>1:
                        #快捷版可能存在两段时间内没有数据，从而进入这里
                        if temp_c_ah[0]==0:
                            print('注：开始时间段内数据缺失，计算暂用0代替') 
                        elif temp_c_ah[-1]==0:
                            print('注：截止时间段内数据缺失，计算暂用0代替')                       
                        #需要计算其累计容量的差值
                        print(['截止累计充放容量',[temp_c_ah[-1],temp_d_ah[-1]]])
                        dc=temp_c_ah[-1]-temp_c_ah[0]
                        dd=temp_d_ah[-1]-temp_d_ah[0]
                        rs.append([dc,dd])
                        if isEndAhCount:
                            end_ah_count.append([temp_c_ah[-1],temp_d_ah[-1]])
                    else:
                        print('选择时间范围内数据完全缺失，请重试！')
            if Flag:
                eba=-1
        if len(rs)==0:
            return False             
        return_rs=[]
        if 'getStrenths' in mode:
            df_rs=pd.DataFrame(rs)
            aver_c_ah,aver_d_ah=df_rs.mean(axis=0)
            aver_cyc=(aver_c_ah+aver_d_ah)/2/ah_sta/self.ratio
            df_cyc=df_rs.mean(axis=1)/ah_sta/self.ratio
            df_rs=pd.concat([df_rs,df_cyc,pd.DataFrame([[aver_c_ah,aver_d_ah,aver_cyc]])],axis=1)
            df_rs.columns=['各簇累计充电容量/Ah','各簇累计放电容量/Ah','各簇等效循环次数/次','平均充/Ah','平均放/Ah','平均等效循环/次']
            if isEndAhCount:
                df_EndAh=pd.DataFrame(end_ah_count)
                df_EndAh.to_excel('./strengths/截至累计Ah数'+self.sta_code+'.xlsx')
            return_rs.append(df_rs)
        if 'getDateTime' in mode:
            return_rs.append(data_date)
        if 'getChargeAh' in mode:
            return_rs.append(all_c_ah)
        if 'getDisChargeAh' in mode:
            return_rs.append(all_d_ah)
        if 'getVols' in mode:
            return_rs.append(all_vols)
        if 'getCurs' in mode:
            #  返回电流值
#             df_curs=pd.DataFrame(all_curs).T
#             df_curs=pd.concat([pd.DataFrame(data_date),df_curs],axis=1)
            return_rs.append(all_curs)
            #return df_curs
        if 'getSocs' in mode:
            return_rs.append(all_socs)
        if 'getWarns' in mode:
            return_rs.append(all_warn)
        if 'getProts' in mode:
            return_rs.append(all_prot)
        return return_rs
    #200002接口，获取PCS数据，频率，有功功率，直流电压、电流，直流功率
    def getPCSData(self,sbox=0,ebox=-1,sba=0,eba=-1):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_p_udc=[]#直流功率
        all_i_udc=[]#直流电流
        all_u_udc=[]#直流电压
        all_pz=[]#有功率
        all_p_rate=[]#电网频率
        data_date=[]
        rs=[]#返回计算后的结果
        for box in range(sbox,ebox):
            if eba==-1:
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                print('-------正在处理第'+str(box+1)+"箱第"+str(ba+1)+"堆的数据----------")
                temp_p_udc,temp_i_udc,temp_u_udc,temp_pz,temp_p_rate,temp_data_date=[],[],[],[],[],[]
                rge=range(len(ds)-1)
                for i in rge:
                    sta_data_date=ds[i]
                    end_data_date=ds[i+1]
                    print('处理中'+sta_data_date)
                    dataJson={
                            "bms_code": self.bmsCodes[box][ba],#['bmsCode'], 
                            "sta_data_date": sta_data_date, 
                            "end_data_date": end_data_date, 
                            "pageNo": 1, 
                            "pageSize": pageSize, #pageNo 1.上午的数据 2.下午的数据
                            "_idAsc": 1
                    }
                    try:
                        result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
                    except:
                        time.sleep(6)
                        result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
    #               print(result)
                    if(result['jszt']=='0' and len(result['data'])>0):
                        temp_p_udc=np.concatenate((temp_p_udc,[it['p_udc'] for it in result['data']]))
                        temp_i_udc=np.concatenate((temp_i_udc,[it['i_udc'] for it in result['data']]))
                        temp_u_udc=np.concatenate((temp_u_udc,[it['u_udc'] for it in result['data']]))
                        temp_pz=np.concatenate((temp_pz,[it['pz'] for it in result['data']]))
                        temp_p_rate=np.concatenate((temp_p_rate,[it['p_rate'] for it in result['data']]))
                        temp_data_date=np.concatenate((temp_data_date,[it['data_date'] for it in result['data']]))
                    all_p_udc.append(temp_p_udc)
                    all_i_udc.append(temp_i_udc)
                    all_u_udc.append(temp_u_udc)
                    all_pz.append(temp_pz)
                    all_p_rate.append(temp_p_rate)
                    data_date.append(temp_data_date)
                rs.append({'data_date':data_date,'p_udc':all_p_udc,'i_udc':all_i_udc,'u_udc':all_u_udc,'pz':all_pz,'p_rate':all_p_rate})
        if len(rs)==0:
            return False
        print(rs)
        return 
    #100001 获取所有电站编码
    def getAllStaCode(self):
        strJson=str({'station_code':'ALL_DOC_CLOU'}) 
        bmsJson=json.loads(self.client.service.getDoc(self.code,self.methods[0],strJson))
        rs=[(it['station_code'],it['station_name']) for it in bmsJson["data"]]
        return rs
    #判断BMU短路开路算法
    def getBMUQS(self,VS,cluster_code,j,pack_bmu_num):
        pack_ord=int(j/pack_bmu_num)+1
        #取单个bmu里边最大电压时刻的12颗电压值
        VSnp=np.array(VS)
        cols=np.argmax(VSnp,axis=1)
        x=np.argmax(np.max(VSnp,axis=1))
        y=cols[x]
#             print([x,y])
#             print(VSnp[x,y])
        #取12串这个时刻电压值
        s=VSnp[:,y]
        if s.max()<3.45:
            #则查找放电末端
            cols=np.argmin(VSnp,axis=1)
            x=np.argmin(np.min(VSnp,axis=1))
            y=cols[x]
            s=VSnp[:,y]
            if s.max()>3.0:
                return False
        print(s)
        isQ=True#假设存在问题
        #问题模式判别 MUX0_1_sc:0&1短路
        if self.comvs(s[1],s[2],s[3]) and self.comvs(s[5],s[6],s[7]) and self.comvs(s[9],s[10],s[11]):
            #是否需要还必须判断其余电芯电压不一样
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与1短路问题"
        elif self.comvs(s[2],s[4],s[6]) and self.comvs(s[3],s[5],s[7]) and s[10]==0 and s[11]==0:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2短路问题"
        elif s[4]==0 and self.comvs(s[4],s[5],s[6],s[7],s[8],s[9],s[10],s[11]):
            #5~12都有问题
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2与3短路问题"
        elif self.comvs(s[0],s[2],s[4],s[8]) and self.comvs(s[1],s[3],s[5],s[9]) and s[6]==0 and self.comvs(s[6],s[7],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2与3都短路问题"
        #开路或者未连接
        elif self.comvs(s[0],s[1]) and self.comvs(s[2],s[3]) and self.comvs(s[4],s[5]) and self.comvs(s[6],s[7]) and self.comvs(s[8],s[9]) and self.comvs(s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0开路问题"
        elif self.comvs(s[0],s[2]) and self.comvs(s[1],s[3]) and self.comvs(s[4],s[6]) and self.comvs(s[5],s[7]) and self.comvs(s[8],s[10]) and self.comvs(s[9],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1开路问题"
        elif self.comvs(s[0],s[4]) and self.comvs(s[1],s[5]) and self.comvs(s[2],s[6]) and self.comvs(s[3],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2开路问题"
        elif self.comvs(s[0],s[8]) and self.comvs(s[1],s[9]) and self.comvs(s[2],s[10]) and self.comvs(s[3],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX3开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3]) and self.comvs(s[4],s[5],s[6],s[7]) and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与1开路问题"
        elif self.comvs(s[0],s[1],s[4],s[5]) and self.comvs(s[2],s[3],s[6],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与2开路问题"
        elif self.comvs(s[0],s[1],s[8],s[9]) and self.comvs(s[2],s[3],s[10],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0与3开路问题"
        elif self.comvs(s[0],s[2],s[4],s[6]) and self.comvs(s[1],s[3],s[5],s[7]) and s[8]==0  and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与2开路问题"
        elif self.comvs(s[0],s[2],s[8],s[10]) and self.comvs(s[1],s[3],s[9],s[11]) and s[4]==0  and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX1与3开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7],s[8],s[9],s[10],s[11]) and s[0]==0:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX2与3开路问题(0,2,3/1,2,3/0,1,2,3)"
        elif self.comvs(s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7]) and s[8]==0 and self.comvs(s[8],s[9],s[10],s[11]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0,1,2开路问题"
        elif self.comvs(s[0],s[1],s[2],s[3],s[8],s[9],s[10],s[11]) and s[4]==0 and self.comvs(s[4],s[5],s[6],s[7]):
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" this is maybe MUX0,1,3开路问题"
        else:
            info=cluster_code+"簇 "+str(pack_ord)+"包"+"BMU"+str(j%pack_bmu_num+1)+" No problems found , OK!"
            isQ=False
        print(info)
        if isQ:
            return [info,s]
        else:
            return False
    #100006 获取电压及温度，并进行相关数据处理,可以详细到具体哪一包数据 quickly 设置是否保存完整数据 False：保存，mode数据处理方法
    def getCellDataByPack(self,sbox=0,ebox=-1,sba=0,eba=-1,scl=0,ecl=-1,sp=0,ep=-1,quickly=True,mode=['getBMUQS']):
        #假如没有输入参数，默认查询所有箱
        if ebox==-1:
            ebox=self.box_num
        pageSize=5000
        ds=self.getDTs(pageSize)
        all_vols,all_Ts=[],[]
#         data_date=[]
        rs,rs_bmu,rs_pack,rs_cl,rs_ba=[],[],[],[],[]#返回BMU问题结果，包、簇、堆温度处理结果
        Flag,Flag_Ts_num=False,True
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
#                 if ba_num1_Flag:
#                     self.ba_num1=eba
#                     ba_num1_Flag=False
                self.ba_num_list.append(eba)
            for ba in range(sba,eba):
                clusterCodes=self.getClusterCodes(box,ba)
                self.cl_num_list.append(len(clusterCodes))
#                 if cl_num1_Flag:
#                     self.cl_num1=len(clusterCodes)
#                     cl_num1_Flag=False
                if clusterCodes is False:
                    return False
                if ecl!=-1 and ecl>scl>=0:
                    clusterCodes=clusterCodes[scl:ecl]
                TS_ba=[]
                for cluster_code in clusterCodes:
                    stt=time.time()
                    print('-------正在处理第'+cluster_code+"的数据----------")
                    dataJson={
                        "bms_code": self.bmsCodes[box][ba], 
                        "cluster_code": cluster_code, 
                        "package_data_order": 1, 
                        "cell_order": 1, 
                        "sta_data_date": '', 
                        "end_data_date": '', 
                        "pageNo": 1, 
                        "pageSize": 4320, #pageNo 1.上午的数据 2.下午的数据
                        "_idAsc": 1
                    }
                    if ep==-1:
                        rgeP=range(self.pack_bmu_num*self.pack_num)
                    else:
                        rgeP=range(self.pack_bmu_num*sp,self.pack_bmu_num*ep)
                    TS_pack,TS_cl=[],[]
                    print(rgeP)
                    #循环每个bmu
                    for j in rgeP:
                        dataJson['package_data_order']=j+1
                        VS,TS=[],[]
                        get_Ts_num=True#温度采集点数量
                        if Flag_Ts_num:
                            Ts_num=0#计数温度采集点
                        a_Ts_num=0#已经计数次数
                        isDataQ=False#数据发生真实异常
                        reMoveRge=[]#可以不用循环的时间
                        for i in range(self.cell_num):
                            #时间循环查询
                            rge=list(range(len(ds)-1))
                            for ir in reMoveRge:
                                try:
                                    rge.remove(ir)
                                except:
                                    pass
                            vols,ts=[],[]
                            for k in rge:
                                sta_data_date=ds[k]
                                end_data_date=ds[k+1]
                                print("..%s"%k,end=' ')
                                dataJson['sta_data_date']=sta_data_date
                                dataJson['end_data_date']=end_data_date
                                dataJson['cell_order']=i+1
                                try:
                                    result=json.loads(self.client.service.getData(self.code,self.methods[5],str(dataJson)))
                                except:
                                    time.sleep(6)
                                    result=json.loads(self.client.service.getData(self.code,self.methods[5],str(dataJson)))
#                                 print(result)
                                if(result['jszt']=='0' and len(result['data']['items'])>0):
                                    vols=np.concatenate((vols,[float(item.get('voltage')) for item in result['data']['items']]))
                                    #前几个有数据，只需循环只需采集点数量
                                    if get_Ts_num:
                                        try:
                                            ts=np.concatenate((ts,[float(item.get('humid')) for item in result['data']['items']]))
                                            if Flag_Ts_num:
                                                Ts_num=Ts_num+1
                                        except:
                                            Flag_Ts_num=False
                                            get_Ts_num=False
                                else:
                                    print('%s~%s'%(sta_data_date,end_data_date))
                                    print(cluster_code+"第%s(~12)串电芯电压没有数据，是否是采集数据有问题！"%(i+1))
                                    isDataQ=True
                                    reMoveRge.append(k)
#                                     break
                            #让异常数据也占位
#                             if len(vols)>0:
                            VS.append(vols)
#                             print('%s:a_Ts_num=a_Ts_num'%a_Ts_num)
                            #完全没数据，默认温度采集点数为3，部分没数据则直接使用的是有数据时的正确数
                            if Ts_num==0:
                                Ts_num=3
                                Flag_Ts_num=True#为了下次还能重新计数
                            if len(ts)>0:
                                TS.append(ts)
                                a_Ts_num=a_Ts_num+1
                            elif isDataQ and a_Ts_num<Ts_num:
                                #有问题则仅急加Ts_num次
                                TS.append(ts)
#                                 print('**************')
#                                 print(TS)
                                a_Ts_num=a_Ts_num+1
                        print('%s bmu'%j)   
                        if quickly==False:
                            #每个bmu数据进行保存，每列是一串
                            all_vols=all_vols+VS
                            all_Ts=all_Ts+TS
                        if 'getBMUQS' in mode and len(VS)>0:
                            r=self.getBMUQS(VS,cluster_code,j,self.pack_bmu_num)
                            if r is not False:
                                rs.append(r)
                        #bmu级别[最高温度，最低温度，最大温差，最大平均温度]
                        if 'getDTempsByBMU' in mode:
                            if len(TS)>0:
                                Tnp=np.array(TS)
                                max_dif_T=max(Tnp.max(axis=0)-Tnp.min(axis=0))
                                max_aver_dif_T=max(np.mean(Tnp,axis=0))
                                #最高温度，最低温度，最大温差，最大平均温度
                                rs_bmu.append([Tnp.max(),Tnp.min(),max_dif_T,max_aver_dif_T])
                            else:
                                rs_bmu.append([np.NaN,np.NaN,np.NaN,np.NaN])
                        #对每个BMU温度数据进行操作,Pack为单位，即合并pack_bmu_num个合并
                        if 'getDTempsByPack' in mode:
                            #将bmu温度数据合并成模组,注：不能为self.pack_bmu_num==1
                            if self.pack_bmu_num==1:
                                TS_pack=TS
                            else:
                                if j%self.pack_bmu_num==0:
                                    TS_pack=[]
                                else:
                                    TS_pack=TS_pack+TS
                            #进行求解最高温度，最大温差，最低温度，最大平均温度 and len(TS_pack)>0
                            if (self.pack_bmu_num==1 or (j+1)%self.pack_bmu_num==0) :
#                                 TS_pack=list(map(list,TS_pack))
                                Tnp=np.array(TS_pack)
                                
#                                 print(Tnp)
#                                 temp_df=pd.DataFrame(TS_pack).T
#                                 print(temp_df)
#                                 max_dif_T=max(temp_df.max(axis=0)-temp_df.min(axis=0))
#                                 #这样子是找每个时刻的采集点温度均值，然后时段取最大值
#                                 #同上存在风险
#                                 #max_aver_dif_T=max(np.mean(Tnp,axis=0))
#                                 max_aver_dif_T=max(temp_df.mean(axis=0))
#                                 rs_pack.append([max(Tnp.max(axis=0)),min(Tnp.min(axis=0)),max_dif_T,max_aver_dif_T])
#                                 Tnp=pd.DataFrame(TS_pack).T
                                if (len(Tnp.max(axis=0))==0 and len(Tnp.min(axis=0))==0):
                                    rs_pack.append([np.NaN,np.NaN,np.NaN,np.NaN])
                                else:
                                    max_dif_T=max(Tnp.max(axis=0)-Tnp.min(axis=0))
                                    max_aver_dif_T=max(np.mean(Tnp,axis=0))
                                    #最高温度，最低温度，最大温差，最大平均温度
                                    rs_pack.append([Tnp.max(),Tnp.min(),max_dif_T,max_aver_dif_T])
                        #以簇为单位，即合并所有pack_bmu_num*pack_num
                        if 'getDTempsByCL' in mode:
                            TS_cl=TS_cl+TS
                        #同理，以堆为单位合并
                        if 'getDTempsByBA' in mode:
                            TS_ba=TS_ba+TS
#                             print(TS_ba)
                    print('------------------')
                    #簇 进行求解最高温度，最大温差，最低温度，最大平均温度
                    if 'getDTempsByCL' in mode and len(TS_cl)>0:
                        TS_cl=list(map(list,TS_cl))
                        Tnp=np.array(TS_cl)
                        if (len(Tnp.max(axis=0))==0 and len(Tnp.min(axis=0))==0):
                            rs_cl.append([np.NaN,np.NaN,np.NaN,np.NaN])
                        else:
                            max_dif_T=max(Tnp.max(axis=0)-Tnp.min(axis=0))
                            max_aver_dif_T=max(np.mean(Tnp,axis=0))
                            rs_cl.append([Tnp.max(),Tnp.min(),max_dif_T,max_aver_dif_T])
                            print("簇编码：%s,\n最高温度：%s℃,最低温度：%s℃,最大温差：%s℃,最大平均温度：%.2f℃"
                                        %(cluster_code,
                                           Tnp.max(),
                                           Tnp.min(),
                                           max_dif_T,
                                           max_aver_dif_T
                            ))
                        edt=time.time()
                        print("查询本簇耗时%s"%(edt-stt))
                        print('-------------------------------')  
                #堆 进行求解最高温度，最大温差，最低温度，最大平均温度
                if 'getDTempsByBA' in mode and len(TS_ba)>0:
                    TS_ba=list(map(list,TS_ba))
#                     print(TS_ba)
                    Tnp=np.array(TS_ba)
#                     print(type(TS_ba))
#                     print(np.max(TS_ba,axis=0))
#                     print(Tnp.min())
                    #这里存在风险，可能不同簇的时间不一致，造成维数不一致，不能直接相减
                    #故考虑，先转化成df
#                     max_dif_T=max(Tnp.max(axis=0)-Tnp.min(axis=0))
                    if (len(Tnp.max(axis=0))==0 and len(Tnp.min(axis=0))==0):
                        rs_ba.append([np.NaN,np.NaN,np.NaN,np.NaN])
                    else:
                        temp_df=pd.DataFrame(TS_ba).T
                        max_dif_T=max(temp_df.max(axis=0)-temp_df.min(axis=0))
                        #这样子是找每个时刻的采集点温度均值，然后时段取最大值
                        #同上存在风险
                        #max_aver_dif_T=max(np.mean(Tnp,axis=0))
                        max_aver_dif_T=max(temp_df.mean(axis=0))
                        rs_ba.append([max(Tnp.max(axis=0)),min(Tnp.min(axis=0)),max_dif_T,max_aver_dif_T])
                print("=======该堆查询完毕==========")                    
            if Flag:
                eba=-1
        if quickly==False:
            f_time_name=self.sta_data_date.split(' ')[0]+'~'+self.end_data_date.split(' ')[0]
            df_bmu_vols=pd.DataFrame(all_vols).T
            df_bmu_ts=pd.DataFrame(all_Ts).T
            #index time;列 每一串 12的倍数列
            df_bmu_vols.to_csv(f_time_name+'_'+self.sta_code+'每个BMU的电压数据.csv')
            df_bmu_ts.to_csv(f_time_name+'_'+self.sta_code+'每个BMU的温度数据.csv')
    
        return_rs=[]
        if 'getBMUQS' in mode and len(rs)>0:
            return_rs.append(rs)
        if 'getDTempsByBMU' in mode and len(rs_bmu)>0:
            return_rs.append(rs_bmu)
        if 'getDTempsByPack' in mode and len(rs_pack)>0:
            return_rs.append(rs_pack)
        if 'getDTempsByCL' in mode and len(rs_cl)>0:
            return_rs.append(rs_cl)
        if 'getDTempsByBA' in mode and len(rs_ba)>0:
            return_rs.append(rs_ba)                     
        return return_rs
    #100009 获取事件数据
    def getEventDataByBAS(self,sbox=0,ebox=-1,sba=0,eba=-1):
        if ebox==-1:
            ebox=self.box_num
        event_df=pd.DataFrame()
        for box in range(sbox,ebox):
            if eba==-1:
                Flag=True
                eba=len(self.bmsCodes[box])
            for ba in range(sba,eba):
                print('-------正在查询第'+str(box+1)+"箱第"+str(ba+1)+"堆的事件数据----------")
                #查询时间段内有多少告警事件
                dataJson={
                                "bms_code": self.bmsCodes[box][ba], 
                                "sta_data_date": self.sta_data_date, 
                                "end_data_date": self.end_data_date, 
                            }
                try:
                    result=json.loads(self.client.service.getData(self.code,self.methods[9],str(dataJson)))
                except:
                    time.sleep(6)
                    result=json.loads(self.client.service.getData(self.code,self.methods[9],str(dataJson)))
        #                                 print(result)
                if(result['jszt']=='0'):
                    
                    for page in range(int(np.ceil(result['record_count']/5000))):
                        dataJson['pageNo']=1+page
                        dataJson['pageSize']=5000
                        dataJson['idAsc']=True
                        try:
                            result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
                        except:
                            time.sleep(6)
                            result=json.loads(self.client.service.getData(self.code,self.methods[10],str(dataJson)))
                        if(result['jszt']=='0' and len(result['data'])>0):
                            temp_df=pd.DataFrame(result['data'])
                            temp_df.insert(1,'bmscode',self.bmsCodes[box][ba])
                            event_df=pd.concat([event_df,temp_df])
        event_df=event_df.reset_index(drop=True)
        return event_df
    def getEMSBat(self):
        inner_sort=['01','02']
        dt = self.getDTs(max_num=4000)
        Bat1 = pd.DataFrame(columns=['01储能','01储能soc'])
        Bat2 = pd.DataFrame(columns=['02储能','02储能soc'])
        for i in range(len(dt)-1):
            strJson1 = str(
            {'station_code':self.sta_code,
             'inner_sort':'01',
             'sta_data_date':dt[i],
             'end_data_date':dt[i+1],
             'pageNo':1,
             'pageSize':5000,
             '_idAsc':1
                }
            )
            strJson2 = str(
            {'station_code':self.sta_code,
             'inner_sort':'02',
             'sta_data_date':dt[i],
             'end_data_date':dt[i+1],
             'pageNo':1,
             'pageSize':5000,
             '_idAsc':1
                }
            )
            BatJson1 = json.loads(self.client.service.getData(self.code,self.methods[10],strJson1))
            BatJson2 = json.loads(self.client.service.getData(self.code,self.methods[10],strJson2))
            minnum = min(len(BatJson1['data']),len(BatJson2['data']))
            maxnum = max(len(BatJson1['data']),len(BatJson2['data']))
            for k in range(minnum):
                Bat1.loc[BatJson1['data'][k]['data_date']] = [BatJson1['data'][k]['pz'],BatJson1['data'][k]['soc']]
                Bat2.loc[BatJson2['data'][k]['data_date']] = [BatJson2['data'][k]['pz'],BatJson2['data'][k]['soc']]
            if len(BatJson1['data'])>len(BatJson2['data']):
                for k in range(minnum,maxnum):
                    Bat1.loc[BatJson1['data'][k]['data_date']] = [BatJson1['data'][k]['pz'],BatJson1['data'][k]['soc']]
            else:
                for k in range(minnum,maxnum):
                    Bat2.loc[BatJson2['data'][k]['data_date']] = [BatJson2['data'][k]['pz'],BatJson2['data'][k]['soc']]
        df1 = Bat1
        df2 = Bat2
        if (df1.empty is False) & (df2.empty is False):
            df = pd.merge(df1,df2,left_index=True,right_index = True,how = 'inner')
        else:
            df = None
        return df,df1,df2
    def getEMSAgc(self,order):
        inner_sort = [order]
        for j in inner_sort:
            dt = self.getDTs(max_num = 4000)
            Agc = pd.DataFrame(columns=[j+'AGC',j+'机组出力',j+'一次调频状态'])
            for i in range(len(dt)-1):
                strJson = str(
                {'station_code':self.sta_code,
                 'inner_sort':j,
                 'sta_data_date':dt[i],
                 'end_data_date':dt[i+1],
                 'pageNo':1,
                 'pageSize':5000,
                 '_idAsc':1
                    }
                )
                AgcJson = json.loads(self.client.service.getData(self.code,self.methods[12],strJson))
                for k in AgcJson['data']:
                    Agc.loc[k['data_date']] = [k['agc_cmd'],k['output'],k['first_frequency']]
        df = Agc
        return df
    def getEMSexist(self):
        inner_sort=['01','02']
        dt = self.getDTs(4000)
        for j in inner_sort:
            strJson = str(
                {'station_code':self.sta_code,
                 'inner_sort':j,
                 'sta_data_date':dt[0],
                 'end_data_date':dt[1]
                    }
                )
            AgcJson = json.loads(self.client.service.getData(self.code,self.methods[-2],strJson))
            try:
                if AgcJson['record_count'] <= 100:
                    print(j+'机组系统无数据')
                    Agc = pd.DataFrame(columns=[j+'AGC',j+'机组出力'])
                else:
                    print(j+'机组系统有数据')
                    Agc = pd.DataFrame(columns=[j+'AGC',j+'机组出力'])
                    Agc.loc[0] = 0,0
            except Exception as a:
                print(a)
                print(j+'机组系统无数据')
                Agc = pd.DataFrame(columns=[j+'AGC',j+'机组出力'])
            finally:
                if j == '01':
                    df1 = Agc
                else:
                    df2 = Agc
        return df1,df2
    
    def __init__(self,sta_data_date, end_data_date,sta_code='0010'):
        '''
        Constructor
        '''
        url = "http://10.13.3.2:7031/DmsService?wsdl"
        self.client = suds.client.Client(url)
        self.client.set_options(timeout=150)
        clouId="CNSYB1"
        nowTime = int(round(time.time()*1000))
        self.code=clouId+str(nowTime)
        self.sta_data_date=sta_data_date
        self.end_data_date=end_data_date
        self.sta_code=sta_code
        strJson=str({'station_code':sta_code})
        bmsJson=json.loads(self.client.service.getDoc(self.code,self.methods[0],strJson))
        self.box_num=bmsJson["data"][0]['box_num']#电站箱子数量
        boxes_data=bmsJson["data"][0]["boxes_data"]
        self.bmsCodes=[[item['bms_code'] for item in boxes_data[box]['bmss_data']] for box in range(self.box_num)]
#         print(self.bmsCodes)

# if __name__=='__main__':

