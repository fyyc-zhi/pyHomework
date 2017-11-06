# -*- coding: utf-8 -*-
from __future__ import division
import  matplotlib.pyplot as plt
import pandas as pd
import time
import numpy as np
import math
import datetime
from datetime import timedelta
import MySQLdb
plt.rcParams['font.sans-serif'] = ['SimHei']

db = MySQLdb.connect("localhost","root","ws123","mytrip" )
cursor = db.cursor()
sql = "SELECT t.record_time,max(t.vehicle_speed),t.engine_rpm FROM mytrip.tb_iov_device_obd_41030402427 t " \
      "where t.device_id='41030402427' GROUP BY t.record_time;"
cursor.execute(sql)
data= cursor.fetchall()
df=pd.DataFrame(list(data),columns=["record_time","vehicle_speed","engine_rpm"])

#计算时间差
times =list(df["record_time"])
diff_time = pd.Series(times[1:]) - pd.Series(times[:-1])
diff_seconds = [x.total_seconds() for x in diff_time]
diff_seconds.insert(0, 0)
df["Time_diff"] = diff_seconds


#数据分段
num = 1
tripNum = []
for diff in df["Time_diff"]:
    if(diff > 600):
        num = num+1
    tripNum.append(num)
df["TripNo"] = tripNum
#数据去噪
df["rationality"] = "good"
df.loc[df["Time_diff"] <= 0, "rationality"] = "bad"
#计算速度和加速度
df["speed"] = df["vehicle_speed"]/3.6
speed = list(df["speed"])
#计算加速度

Acceleration = list(pd.Series(speed[1:]) - pd.Series(speed[:-1]))
Acceleration.insert(0, 0)
Acceleration = np.array(Acceleration)/np.array(diff_seconds)
df["Acceleration"] = Acceleration
df["ABS_Acceleration"] = abs(Acceleration)
df["Speed_Flag"] = ' NaN'
#将每条数据里的速度根据不同范围进行标记
df.loc[df["speed"] < 5.6, "Speed_Flag"] = 'a'
df.loc[(df["speed"] >= 5.6) & (df["speed"] < 11.1), "Speed_Flag"] = 'b'
df.loc[(df["speed"] >= 11.1) & (df["speed"] < 16.7), "Speed_Flag"] = 'c'
df.loc[df["speed"] > 16.7, "Speed_Flag"] = 'd'
#计算每个trip中不同标记的数量
#If((Trip_No==1)&&( Speed_ Flag ==a)) Speed_Classify[1][a]++;
Speed_Classify = np.zeros((92, 4))
for i in range(0,  df.__len__()):
    if(df.at[i, "Speed_Flag"] == 'a'):
        y = 0
    if (df.at[i, "Speed_Flag"] == 'b'):
        y = 1
    if (df.at[i, "Speed_Flag"] == 'c'):
        y = 2
    if (df.at[i, "Speed_Flag"] == 'd'):
        y = 3
    x = df.at[i, "TripNo"]
    Speed_Classify[x-1, y] += 1

#将每条数据里的加速度根据不同范围进行标记
df["ABS_Acceleration_Flag"] = "NaN"
df.loc[df["ABS_Acceleration"] < 0.1, "ABS_Acceleration_Flag"] = 'a'
df.loc[(df["ABS_Acceleration"] >= 0.1) & (df["ABS_Acceleration"] < 0.3), "ABS_Acceleration_Flag"] = 'b'
df.loc[df["ABS_Acceleration"] >= 0.3, "ABS_Acceleration_Flag"] = 'c'


#计算每个trip中不同标记的数量
ABS_Acceleration_Classify = np.zeros((92, 3))
for i in range(0,  df.__len__()):
    if(df.at[i, "ABS_Acceleration_Flag"] == 'a'):
        y = 0
    if (df.at[i, "ABS_Acceleration_Flag"] == 'b'):
        y = 1
    if (df.at[i, "ABS_Acceleration_Flag"] == 'c'):
        y = 2
    x = df.at[i, "TripNo"]
    ABS_Acceleration_Classify[x-1, y] += 1

#将每条数据里的转速根据不同范围进行标记
df["RPM_Flag"] = "NaN"
df.loc[df["engine_rpm"] < 1000, "RPM_Flag"] = 'a'
df.loc[(df["engine_rpm"] >= 1000) & (df["engine_rpm"] < 2000), "RPM_Flag"] = 'b'
df.loc[df["engine_rpm"] >= 2000, "RPM_Flag"] = 'c'

#计算每个trip中不同标记的数量
RPM_Classify = np.zeros((92, 3))
for i in range(0,  df.__len__()):
    if(df.at[i, "RPM_Flag"] == 'a'):
        y = 0
    if (df.at[i, "RPM_Flag"] == 'b'):
        y = 1
    if (df.at[i, "RPM_Flag"] == 'c'):
        y = 2
    x = df.at[i, "TripNo"]
    RPM_Classify[x-1, y] += 1


#计算不同trip的数据记录数，以及速度、加速度、转速均值
Trip_Num = np.zeros(num)
speed_tot = np.zeros(num)
ABS_Acceleration_tot = np.zeros(num)
RPM_tot = np.zeros(num)
Average_speed = np.zeros(num)
Average_ABS_Acceleration = np.zeros(num)
Average_RPM = np.zeros(num)

for i in range(0,  df.__len__()):
    x = df.at[i, "TripNo"]
    Trip_Num[x-1] += 1
    speed_tot[x-1] += df.at[i, "speed"]
    if( not math.isnan(df.at[i, "ABS_Acceleration"])):
        ABS_Acceleration_tot[x-1] += df.at[i, "ABS_Acceleration"]
    RPM_tot[x-1] += df.at[i, "engine_rpm"]
for i in range(0,  num):
    Average_speed[i] = speed_tot[i] / Trip_Num[i]
    Average_ABS_Acceleration[i] = ABS_Acceleration_tot[i] / Trip_Num[i]
    Average_RPM[i] = RPM_tot[i] / Trip_Num[i]
#计算不同trip的速度、加速度、转速方差
speed_v = np.zeros(num)
ABS_Acceleration_v = np.zeros(num)
RPM_v = np.zeros(num)
Variance_speed = np.zeros(num)
Variance_ABS_Acceleration = np.zeros(num)
Variance_RPM = np.zeros(num)

for i in range(0,  df.__len__()):
    x = df.at[i, "TripNo"] -1
    speed_v[x] += np.square(df.at[i, "speed"] - Average_speed[x])
    if (not math.isnan(df.at[i, "ABS_Acceleration"])):
        ABS_Acceleration_v[x] += np.square(df.at[i, "ABS_Acceleration"] - Average_ABS_Acceleration[x])
    RPM_v[x] += np.square(df.at[i, "engine_rpm"] - Average_RPM[x])

for i in range(0,  num):
    Variance_speed[i] = speed_v[i] / Trip_Num[i]
    Variance_ABS_Acceleration[i] = ABS_Acceleration_v[i] / Trip_Num[i]
    Variance_RPM[i] = RPM_v[i] / Trip_Num[i]






#特征选择
v_a= np.zeros(num)
v_b= np.zeros(num)
v_c= np.zeros(num)
v_d = np.zeros(num)
a_a = np.zeros(num)
a_b = np.zeros(num)
a_c = np.zeros(num)
r_a = np.zeros(num)
r_b = np.zeros(num)
r_c = np.zeros(num)
for i in range(0,  num):
    v_a[i] = df[(df["TripNo"]==i+1)&(df["Speed_Flag"]=='a')].__len__() / Trip_Num[i]
    v_b[i] = df[(df["TripNo"] == i + 1) & (df["Speed_Flag"] == 'b')].__len__() / Trip_Num[i]
    v_c[i] = df[(df["TripNo"] == i + 1) & (df["Speed_Flag"] == 'c')].__len__() / Trip_Num[i]
    v_d[i] = df[(df["TripNo"] == i + 1) & (df["Speed_Flag"] == 'd')].__len__() / Trip_Num[i]
    a_a[i] = df[(df["TripNo"] == i + 1) & (df["ABS_Acceleration_Flag"] == 'a')].__len__() / Trip_Num[i]
    a_b[i] = df[(df["TripNo"] == i + 1) & (df["ABS_Acceleration_Flag"] == 'b')].__len__() / Trip_Num[i]
    a_c[i] = df[(df["TripNo"] == i + 1) & (df["ABS_Acceleration_Flag"] == 'c')].__len__() / Trip_Num[i]
    r_a[i] = df[(df["TripNo"] == i + 1) & (df["RPM_Flag"] == 'a')].__len__() / Trip_Num[i]
    r_b[i] = df[(df["TripNo"] == i + 1) & (df["RPM_Flag"] == 'b')].__len__() / Trip_Num[i]
    r_c[i] = df[(df["TripNo"] == i + 1) & (df["RPM_Flag"] == 'c')].__len__() / Trip_Num[i]
results = pd.DataFrame()
results["Trip"] = pd.Series(range(1, num+1))
results["v_avg"] = pd.Series(Average_speed)
results["v_std"] = pd.Series(Variance_speed)
results["v_a"] = pd.Series(v_a)
results["v_b"] = pd.Series(v_b)
results["v_c"] = pd.Series(v_c)
results["v_d"] = pd.Series(v_d)
results["a_avg"] = pd.Series(Average_ABS_Acceleration)
results["a_std"] = pd.Series(Variance_ABS_Acceleration)
results["a_a"] = pd.Series(a_a)
results["a_b"] = pd.Series(a_b)
results["a_c"] = pd.Series(a_c)
results["r_avg"] = pd.Series(Average_RPM)
results["r_std"] = pd.Series(Variance_RPM)
results["r_a"] = pd.Series(r_a)
results["r_b"] = pd.Series(r_b)
results["r_c"] = pd.Series(r_c)




# If(Trip_No==1) {
# speed_v[1]+=;
# ABS_Acceleration_v[1]+=;
# RPM_v[1]+= ;}}
# Variance_speed[1]= ;
# Variance_ABS_Acceleration [1]= ;
# Variance_RPM [1]= ;




print results