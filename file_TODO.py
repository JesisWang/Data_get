# -*- coding: utf-8 -*-
'''
Created on 2019年5月14日
文件处理
@author: Administrator
'''
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly


# df=pd.read_csv('2019-04-16~2019-04-16_0010#4cl_Ts.csv',index_col=0)
# T1,T2,T3=[],[],[]
# for i in range(0,df.shape[1],12):
#     print(i)
#     MaxTs=np.max(df.iloc[:,i:(12+i)], axis=1)
#     MinTs=np.min(df.iloc[:,i:(12+i)], axis=1)
#     difTs=MaxTs-MinTs
#     T1.append(MaxTs)
#     T2.append(MinTs)
#     T3.append(difTs)
# 
# rs_df=pd.DataFrame(T1).T
# print(rs_df)
# rs_df=pd.concat([rs_df,pd.DataFrame(T2).T],axis=1)
# rs_df=pd.concat([rs_df,pd.DataFrame(T3).T],axis=1)
# rs_df.to_excel('4月18日延农5-2-2各模组温度.xlsx')
df=pd.read_excel('4月16日延农1-2-2各模组温度.xlsx',skiprows=1,index_col=0)
df.columns=range(df.shape[1])
data=[0,0,0,0,0,0]
nmso=['S-最高温度','S-最低温度','S-最大温差','O-最高温度','O-最低温度','O-最大温差']
max_Ts_df_sp=df.iloc[:,0:7]
min_Ts_df_sp=df.iloc[:,19:26]
max_Ts_df_ot=df.iloc[:,7:19]
min_Ts_df_ot=df.iloc[:,26:38]
# dif_Ts_df=df.iloc[:,38:57]
data[0]=max_Ts_sp=max_Ts_df_sp.max(axis=1)
data[1]=min_Ts_sp=min_Ts_df_sp.min(axis=1)
data[2]=dif_Ts_sp=max_Ts_sp-min_Ts_sp

data[3]=max_Ts_ot=max_Ts_df_ot.max(axis=1)
data[4]=min_Ts_ot=min_Ts_df_ot.min(axis=1)
data[5]=dif_Ts_ot=max_Ts_sp-min_Ts_ot

print(max_Ts_sp)

colors = ['#8dd3c7', '#d62728', '#ffffb3', '#bebada',
          '#fb8072', '#80b1d3', '#fdb462',
          '#b3de69', '#fccde5', '#d9d9d9',
          '#bc80bd', '#ccebc5', '#ffed6f']
colorsX = ['#1f77b4', '#d62728', '#9467bd', '#ff7f0e', '#bebada', '#660033','#b3de69', '#fccde5', '#d9d9d9',
          '#bc80bd', '#ccebc5', '#ffed6f']
traces = []
print(data[0])
for i in range(0, 1):
    nm = nmso[i] 
    traces.append(go.Scatter(
        # mode='lines',line=dict(color='rgba(0,255,0,0)', width=0.5),
        mode='lines', line=dict(color=colorsX[i], width=0.7),
        connectgaps=False,  # 对于缺数据断点是否连接曲线  #x=df['time_stamp'],     对x轴利用Pandas赋值
        x=list(range(len(data[i]))),
        y=data[i],  # 对y轴利用Pandas赋值
        yaxis='y',  # 标注轴名称
        name=nm,  # 标注鼠标移动时的显示点信息
        hovertext=nm,
#         text=['电压1', '电压2'],
        marker=dict(color=colorsX[i], size=12, ),
        legendgroup=i+1,
        showlegend=True,
        visible='legendonly' if i == 2 or i==5 else True ,
    ))
# '''定义layout'''
layout = go.Layout(
#         width=1920,
#     legend=dict(x=0.5,y=1.1,font=dict(size=15),orientation='v'),
    legend=dict(y=0.5),
    xaxis=dict(
        domain=[0.1, 0.9],
        showline=True,
        showgrid=True,
        showticklabels=True,
        # linecolor=self.colorsX[0],
        linewidth=2,
#             autotick=True,
        ticks='outside',
        # tickcolor=self.colorsX[0],
        tickwidth=2,
        ticklen=5,
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
#         hoverformat="%Y/%m/%d %H:%M:%S",
    ),
    # 第一个y轴
    yaxis=dict(
        title='温度(摄氏度)',
        linecolor=colorsX[0],
        showgrid=True,
        zeroline=False,  # 是否显示基线,即沿着(0,0)画出x轴和y轴
        showline=True,
        showticklabels=True,
        titlefont=dict(color=colorsX[0]),
        tickfont=dict(color=colorsX[0]),
        # tick0=0,
        dtick=1,

    ),
    # autosize=True,
    hovermode='closest',
    margin=dict(
        autoexpand=False,
        l=20,
        r=20,
        t=100,
    ),
    showlegend=True,
)

annotations = []
# '''增加annotations注释的文本格式'''
# Title
annotations.append(dict(xref='paper', yref='paper', x=0.0, y=1.05,
                        xanchor='left', yanchor='bottom',
                        text=str('2019-4.16延农不开空调 1-2-2温度分析图'),
                        font=dict(family='Arial',
                                  size=30,
                                  color='rgb(37,37,37)'),
                        showarrow=False))
layout['annotations'] = annotations
fig = go.Figure(data=traces, layout=layout)
plot_url = plotly.offline.plot(fig)