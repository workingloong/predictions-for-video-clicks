# predict the clicks in the 8th week according to the 7th weeks by least square
# date: 2016/01/08
# author: workingloong

import scipy.io as sio
import numpy as np
import pickle
from pyspark import SparkContext
from pylab import *
from pyspark.mllib.recommendation import ALS,Rating

def leastsq(y):
	x = [1,2,3,4,5,6,7]
	meanx = sum(x) / len(x)   
    	meany = sum(y) / len(y)   
    	xsum = 0.0
    	ysum = 0.0
    	for i in range(len(x)):
        		xsum += (x[i] - meanx)*(y[i]-meany)
        		ysum += (x[i] - meanx)**2
    	k = xsum/ysum
    	b = meany - k*meanx

    	return k,b  

def cal_leastsq(r):
	k,b = leastsq(r)
	p1 = int(round(k*8 + b))
	p2 = int(round(k*9+b))
	if p1<0: p1 = 0
	if p2<0: p2 = 0
	result = [p1, p2 ,p2 ,p2 ,p2 ,p2 ,p2]
	return result

def cal(r):
	num = [];
	for i in range(len(r)):
		if r[i]:
			num.append(1)
		else:
			num.append(0)
	if (sum(num)>4 & sum(num[3:7]) >3) :
		p = int(round(sum(r)/7))
		result = [p, p ,p ,p ,p ,p ,p]
	elif (sum(num)>4 & sum(num[3:7]) >2):
		p = int( round( sum(r[3:7])/4) )
		result = [p, p ,p ,p ,p ,p ,p]
	else:
		result= [0,0,0,0,0,0,0]
	return result

sc = SparkContext("local[4]","First Spark App")   
user_data = sc.textFile("/home/workingloong/spark_python/data_6_weeks_7_result.txt") # create a RDD of spark

line_num = user_data.count()
user_num =  line_num/10

data =  user_data.map(lambda line : line.split(',') ).map(lambda r : [ float(r[0]),float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), float(r[6]) ])
data_d7 = data.map(lambda r : cal_leastsq(r)).collect()

f= open('pred_1_week7.txt','a')
for  i in range(0,user_num):
	temp = data_d7[10*i: 10*(i+1)]
	pred =''
	#num = []
	for m  in range(0,7):
		for n in range(0,10):
			pred = pred+str(int(temp[n][m])) + ','
	f.write(pred[0:-1])
	f.write('\r\n')
