#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pandas as pd
import numpy as np
import MySQLdb
conn = MySQLdb.connect("localhost","root","ws123","mytrip" )
cur = conn.cursor()

# 使用execute方法执行SQL语句
sql = "SELECT t.record_time,max(t.vehicle_speed),t.engine_rpm FROM mytrip.tb_iov_device_obd_41030402427 t " \
      "where t.device_id='41030402427' GROUP BY t.record_time;"
cur.execute(sql)
data = cur.fetchall()

#print data
# 关闭数据库连接
conn.close()
df=pd.DataFrame(list(data),columns=["record_time","vehicle_speed","engine_rpm"])

#数据分段（Trip Extraction）
time=np.array(df['record_time'])
time=np.diff(time)/np.timedelta64(1,'s')
df['Time_diff']=0
df.loc[1:,['Time_diff']]=time
df['Trip_No']=0;
temp=1
for i in range(0,df.__len__()):
        if df.at[i,'Time_diff']>600:
            temp=temp+1
            df.at[i, 'Trip_No']=temp
        else:
            df.at[i, 'Trip_No'] = temp
#数据去噪
df['BAD']=False
for i in range(0, df.__len__()):
    if df.at[i, 'Time_diff'] > 0:
        df.at[i, 'BAD'] = False
    else:
        df.at[i, 'BAD'] = True

#数据预处理
df['vehicle_speed']=df['vehicle_speed']/3.6
ABS_Acceleration=np.array(df['vehicle_speed'])
ABS_Acceleration=np.diff(ABS_Acceleration)/time
df['ABS_Acceleration']=0;
df.loc[1:,['ABS_Acceleration']]=ABS_Acceleration
#计算每个trip里速度分布情况
df['Speed_Flag']='e'
for i in range(0, df.__len__()):
    if (df.at[i, 'vehicle_speed'] < 5.6):
        df.at[i, 'Speed_Flag'] = 'a'
    if (df.at[i, 'vehicle_speed'] < 11.1)&(df.at[i, 'vehicle_speed']>=5.6):
        df.at[i, 'Speed_Flag'] = 'b'
    if (df.at[i, 'vehicle_speed'] < 16.7) &(df.at[i, 'vehicle_speed'] >=11.1):
        df.at[i, 'Speed_Flag'] = 'c'
    if (df.at[i, 'vehicle_speed'] >= 16.7):
        df.at[i, 'Speed_Flag'] = 'd'
#计算每个trip中不同标记的数量
Speed_Classify=[[0] * 4] * 92
for i in range(0, df.__len__()):
    x=df.at[i, 'Trip_No']-1
    if df.at[i, 'Speed_Flag'] =='a':
        y=0
    if df.at[i, 'Speed_Flag'] =='b':
        y=1
    if df.at[i, 'Speed_Flag'] =='c':
        y=2
    if df.at[i, 'Speed_Flag'] =='d':
        y=3
    Speed_Classify[x][y]+=1
#计算每个trip里加速度分布情况
df['ABS_Acceleration_Flag']='e'
for i in range(0, df.__len__()):
    if (df.at[i, 'ABS_Acceleration'] < 0.1):
        df.at[i, 'ABS_Acceleration_Flag'] = 'a'
    if (df.at[i, 'ABS_Acceleration'] < 0.3)&(df.at[i, 'ABS_Acceleration']>=0.1):
        df.at[i, 'ABS_Acceleration_Flag'] = 'b'
    if (df.at[i, 'ABS_Acceleration'] >=0.3):
        df.at[i, 'ABS_Acceleration_Flag'] = 'c'
#计算每个trip中不同标记的数量
ABS_Acceleration_Classify=[[0] * 3] * 92
for i in range(0, df.__len__()):
    x=df.at[i, 'Trip_No']-1
    if df.at[i, 'ABS_Acceleration_Flag'] =='a':
        y=0
    if df.at[i, 'ABS_Acceleration_Flag'] =='b':
        y=1
    if df.at[i, 'ABS_Acceleration_Flag'] =='c':
        y=2
    ABS_Acceleration_Classify[x][y]+=1
#计算每个trip里转速分布情况
df['RPM_Flag']='e'
for i in range(0, df.__len__()):
    if (df.at[i, 'engine_rpm'] < 1000):
        df.at[i, 'RPM_Flag'] = 'a'
    if (df.at[i, 'engine_rpm'] < 2000)&(df.at[i, 'engine_rpm']>=1000):
        df.at[i, 'RPM_Flag'] = 'b'
    if (df.at[i, 'engine_rpm'] >=2000):
        df.at[i, 'RPM_Flag'] = 'c'

#计算每个trip中不同标记的数量
RPM_Classify=[[0] * 3] * 92


#统计不同trip的数据记录数、以及速度、加速度、转速的均值与方差
#print df




print df