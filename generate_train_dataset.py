# generate the train dataset for machine learning model
# feature = [sum(click_week1),sum(click_week2),sum(click_week3),.....sum(click_week6)]
# Date: 2016/01/06
# author: workingloong

import scipy.io as sio
import numpy as np
from pyspark import SparkContext
from pylab import *
from pyspark.mllib.recommendation import ALS,Rating

def extract_feature(r):
	feature_label = []
	r = map(int ,r)  # r[-1]  starnds for the result whether the user click the video in the 7th week
	for i in range(7):
		num = 0
		for j in range(7*i ,7*(i+1)):
			if r[j]:
				num +=1
		feature_label.append( num )
		if i==5:
			feature_label.append(sum(r[7*i :7*(i+1)]))
	return feature_label

def extract_feature_1(train_data):
	num_line = len(train_data)
	feature_label = []
	for i in range(num_line/10):
		v0 = np.array(train_data[10*i])
		v1 = np.array(train_data[10*i+1])
		v2 = np.array(train_data[10*i+2])
		v3 = np.array(train_data[10*i+3])
		v4 = np.array(train_data[10*i+4])
		v5 = np.array(train_data[10*i+5])
		v6 = np.array(train_data[10*i+6])
		v7 = np.array(train_data[10*i+7])
		v8 = np.array(train_data[10*i+8])
		v9 = np.array(train_data[10*i+9])
		num = v0+v1+v2+v3+v4+v5+v6+v7+v8+v9
		feature_label.append(list(num))
	return feature_label

sc = SparkContext("local[4]","First Spark App")   
user_data = sc.textFile("/home/workingloong/spark_python/data_1_7_weeks.txt") # create a RDD of spark
print user_data.first()  # print the first line of the data  set

#  create a new RDD including ()
data = user_data.map(lambda line : line.split(","))
data1 = data.map(lambda r : extract_feature(r))

train_data = data1.collect()

f = open('pre_deal_data_3.txt','a')
# feature = [sum(click_week1),sum(click_week2),sum(click_week3),.....sum(click_week6)]
for line in train_data :
	data_line =''
	for i in line:
		data_line = data_line + str(i)+','
	f.write(data_line[0:-1])
	f.write('\r\n')
f.close()
