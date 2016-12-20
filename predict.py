# generate the click predictions by trained RF model
# combine the predicted clicks and regression predictions by least square
# date: 2016/01/08
# author: workingloong

import pyspark
import pickle
import scipy.io as sio
import numpy as np
from pylab import *
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree,DecisionTreeModel
from pyspark.mllib.tree import RandomForest, RandomForestModel

def click_decision(r):
	num_day = []
	num_click = []
	for i in range(len(r)):
		num_click.append(int(r[i]))
		if int(r[i]):	
			num_day.append(1)
		else:
			num_day.append(0)
	if (sum(num_day )>=3):
		if( sum(num_day[28:49]) > 0 ):
			return 1
		#elif sum(num_click) > 50:
		#	return 1
		else:
			return 0 
	else:
		return 0


def extract_feature(r):
	feature = []
	r = map(int ,r)  # r[-1]  starnds for the result whether the user click the video in the 7th week
	for i in range(6):
		num = 0
		for j in range(7*i ,7*(i+1)):
			if r[j]:
				num +=1
		feature.append( num )
		if i==5:
			feature.append(sum(r[7*i :7*(i+1)]))
	return feature

def  tree_decision(r):
	r = float(r)
	if r >0.28:
		return 1
	else:
		return 0

sc = SparkContext("local[8]","First Spark App")
user_data = sc.textFile("/home/workingloong/spark_python/data_2_7_weeks.txt")   #  create a RDD 
#user_data = sc.textFile("/home/workingloong/part1-r")
print user_data.first()

#  create a new RDD including ()
data = user_data.map(lambda line : line.split(","))
data_feature = data.map(lambda r : extract_feature(r))
model = RandomForestModel.load(sc, "RandomForestModelPath_30_9") # 30 trees ,maxDepth = 7
#print "Decision Tree number of nodes : "+ str(model.numNodes())

data_reg = model.predict(data_feature.map(lambda r : r))
data_click = data_reg.map(lambda r : tree_decision(r)).collect()

print data_click[1:100]

pkl_file = open('user_dict.pkl','rb');
all_userID_dict = pickle.load(pkl_file)
all_users_num = 299320;

week7= sc.textFile('pred_1_week7.txt')
data7 = week7.map(lambda line : line.split(',')).collect()

# save the predicts in the txt file
f = open('predicts.txt','a')
for u in range(0,all_users_num):
#for u in range(1,2):
	predicts = ''
	temp = []
	for w in range(0,7):
		for v in range(0,10):
			if  int(data_click[10*u + v]):
				if  int(data7[u][10*(w) + v]):
					pred_num = int(data_click[10*u + v])  *  int(data7[u][10*(w) + v])
				else:
					pred_num = int(data_click[10*u + v])  +  int(data7[u][10*(w) + v])
			else:
				pred_num = 0
			predicts = predicts +str( pred_num )+','
			temp.append(  pred_num )
	if sum(temp):
		f.write('%s\t'%all_userID_dict[u])
		f.write(predicts[0:-1])
		f.write('\r\n')
f.close()