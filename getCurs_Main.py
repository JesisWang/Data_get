# -*- coding: utf-8 -*-
'''
Created on 2019年5月9日
下一步计划，插入多线程函数，可选模式，以簇或堆或箱或电站进行多线程
@author: 逻辑的使命
'''
from getAPI import getStaWork
import pandas as pd
from mkdir import mkdir

sta_data_date="2019-04-19 00:00:00"
end_data_date="2019-04-19 12:00:00"

if __name__ == '__main__':
    sta_config=pd.read_excel('./assets/sta_cl_ah_config.xlsx')
    sta_codes,sta_names=sta_config['code'],sta_config['name']
    sta_i=1#云河编号
    obj=getStaWork(sta_data_date, end_data_date,sta_code=sta_codes[sta_i])
    mkdir('./curs_datas/'+sta_names[sta_i])
    
    sbox,ebox=1,2
    sba,eba=0,2
    scl,ecl=0,3
    mode=['getDateTime','getCurs']
    
    rs=obj.getDataByCLS(quickly=False,sbox=sbox,ebox=ebox,sba=sba,eba=eba,scl=scl,ecl=ecl,mode=mode)
    if len(rs)>1:
        rs_time=rs[0]
        rs_curs=rs[1]
        for i in range((ebox-sbox)*(eba-sba)):
            df_time=pd.DataFrame(rs_time[i*(ecl-scl)])
            df_curs=pd.DataFrame(rs_curs[i*(ecl-scl):(i+1)*(ecl-scl)]).T
            rs=pd.concat([df_time,df_curs],axis=1)
            cols=[]
            cols.append('时间')
            for j in range(scl,ecl):
                cols.append('%s箱%s堆%s簇'%(sbox+1+int(i/(eba-sba)),sba+1+i%(eba-sba),1+j))
            rs.columns=cols
            print(rs)
            if ecl-scl==1:
                c='%s'%(scl+1)
            else:
                c='%s_%s'%(scl+1,ecl)
            f_time_name=sta_data_date.split(' ')[0]+'~'+end_data_date.split(' ')[0]
            rs.to_csv('./curs_datas/'+sta_names[sta_i]+'/%s_电流数据%s-%s-%s.csv'%(f_time_name,sbox+1+int(i/(eba-sba)),sba+1+i%(eba-sba),c),index=False)
