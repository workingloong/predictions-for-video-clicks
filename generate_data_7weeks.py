# generate the data of 7 weeks as time order
# the line is ID of vedio
# the arrow is the day of week
# Date: 2016/01/05
# author: workingloong

import scipy.io as sio
import numpy as np
from pyspark import SparkContext
from pylab import *
from pyspark.mllib.recommendation import ALS,Rating

def video_week(u, v , preds):
	predicts = ''
	for w in range(1,8):
		vw = int (str (v)+str(w))
		if  (u,vw) in preds:
			pred_num = int (round(preds[(u,vw)])) 
			predicts = predicts +str( pred_num )+','
		else:
			predicts = predicts + '0,'
	return predicts

def click_result(u,v,preds):
	pred_num = 0
	for w in range(1,8):
		vw= int (str (v)+str(w))
		if  (u,vw) in preds:
			pred_num = pred_num + int (round(preds[(u,vw)]))
	if pred_num>0:
		return str(1)+','
	else:
		return str(0)+','

def click_sum(u,v,preds):
	pred_num = 0
	for w in range(1,8):
		vw= int (str (v)+str(w))
		if  (u,vw) in preds:
			pred_num = pred_num + int (round(preds[(u,vw)]))
	return str(pred_num)+','

sc = SparkContext("local[4]","First Spark App")   
user_data = sc.textFile("/home/workingloong/spark_python/tianyi.txt") # create a RDD of spark
#user_data = sc.textFile("/home/workingloong/part1-r")
print user_data.first()  # print the first line of the data  set

#create a new RDD consists of  [user_id, weekday_id, video_id, clicks], such as [+E8hJNnW23U=	   d3	v10	5 ]
data = user_data.map(lambda line : line.split("\t")).map(lambda  record : (record[0],record[1][0:2],record[1][2:],record[2],record[3]))
print data.first()
data.cache()  # cache the RDD for later use

# coding the video, such as v1:1, v2 :2., v3:3.....
all_video = data.map(lambda record : record[3]).distinct().collect() # distinct the weekday, the function collect() return a list consisted of the weekday (d1 to d7)
all_video_dict = {}
for video in all_video:
	all_video_dict[video] = int(video[1:])
for video in all_video:
	print "the encoding of %s is %d"%(video,all_video_dict[video])

#coding the weekday, such as d1 :1, d2:2......
all_weeks = data.map(lambda record : record [1]).distinct().collect()
all_weeks.sort()
idx = 1;
all_weeks_dict = {}
for week in all_weeks:
	all_weeks_dict[week] = idx
	idx += 1

#coding the weekday, such as d1 :1, d2:2......
all_weekdays = data.map(lambda record : record [2]).distinct().collect()
all_weekdays.sort()
idx = 1;
all_weekdays_dict = {}
for weekday in all_weekdays:
	all_weekdays_dict[weekday] = idx
	idx += 1

#coding the user_ID
all_users = data.map(lambda record : record [0]).distinct().collect()
all_users.sort()
idx = 1;
all_users_dict = {}
all_userID_dict = {}
for user in all_users:
	all_users_dict[user] = idx
	all_userID_dict[idx] = user
	idx += 1
users_num = idx -1   # the count of users in the data set
data_d = data.map(lambda record : (int(all_users_dict[record[0]]), all_weeks_dict[record[1]], all_weekdays_dict[record[2]], all_video_dict[record[3]], float(record[4])))

data_d1 = data_d.filter(lambda r : r[1]  == 1)
data_d2 = data_d.filter(lambda r : r[1]  == 2)
data_d3 = data_d.filter(lambda r : r[1]  == 3)
data_d4 = data_d.filter(lambda r : r[1]  == 4)
data_d5 = data_d.filter(lambda r : r[1]  == 5)
data_d6 = data_d.filter(lambda r : r[1]  == 6)
data_d7 = data_d.filter(lambda r : r[1]  == 7)

data_train1 = data_d1.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train2 = data_d2.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train3 = data_d3.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train4 = data_d4.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train5 = data_d5.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train6 = data_d6.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
data_train7 = data_d7.map(lambda record : ((record[0], int (str(record[3]) + str(record[2])) ) , float(record[4]))).collect()
print '+++++++++++++++++++++'

preds_1 ={}
for tu in data_train1:
	preds_1[ tu[0]] = tu[1]
preds_2 ={}
for tu in data_train2:
	preds_2[ tu[0]] = tu[1]
preds_3 ={}
for tu in data_train3:
	preds_3[ tu[0]] = tu[1]
preds_4 ={}
for tu in data_train4:
	preds_4[ tu[0]] = tu[1]
preds_5 ={}
for tu in data_train5:
	preds_5[ tu[0]] = tu[1]
preds_6={}
for tu in data_train6:
	preds_6[ tu[0]] = tu[1]
preds_7={}
for tu in data_train7:
	preds_7[ tu[0]] = tu[1]

# save the result in the txt file
f = open('data_6_weeks_7_result.txt','a')
for u in range(1,users_num+1):
	for v in range(1,11):
		predicts = ''
		predicts = predicts + video_week(u,v,preds_1)
		predicts = predicts + video_week(u,v,preds_2)
		predicts = predicts + video_week(u,v,preds_3)
		predicts = predicts + video_week(u,v,preds_4)
		predicts = predicts + video_week(u,v,preds_5)
		predicts = predicts + video_week(u,v,preds_6)
		#predicts = predicts + video_week(u,v,preds_7)
		predicts = predicts + click_result(u,v,preds_7)  # the result whether the user click the video in 7th week, 1 stands for 'yes',

		f.write(predicts[0:-1])
		f.write('\r\n')
f.close()