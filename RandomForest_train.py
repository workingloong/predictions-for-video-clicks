# training data by RandomForest model
# date: 2016/01/08
# author: workingloong

import scipy.io as sio
import numpy as np
import pickle
from pyspark import SparkContext
from pylab import *
import matplotlib.pyplot as plt
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree,DecisionTreeModel
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.tree import RandomForest, RandomForestModel

def extract_features_dt(record):
	""" extract the vector of features"""
	return np.array(map(int, record[0:-1]))
def extract_label_dt(record):
	return int(record[-1])
def squared_error(actual,pred):
	return (actual - pred)**2
def abs_error(actual,pred):
	return np.abs((actual - pred))
def squared_log_error(actual,pred):
	return (np.log(pred +1) - np.log(actual +1))**2
def extract_user(record,user):
	if record[0] == user:
		print '************'
		return (record[1],record[2],record[3])

def evaluate_dt(train, test, maxDepth, maxBins):
#"evaluate the performance of different maxDepth or maxBins"
	model = DecisionTree.trainRegressor(train,{},
		impurity = 'variance', maxDepth = maxDepth,maxBins = maxBins)
	preds = model.predict(test.map(lambda p : p.features))
	actual = test.map(lambda p: p.label )
	tp = actual.zip(preds)
	rmsle = np.sqrt(tp.map(lambda (t,p) : squared_log_error(t,p)).mean())
	return rmsle

def click_decision(r):
	r = float(r)
	if r >0.22:
		return 1
	else:
		return 0

def evaluate_pred(LP):
	num_right = 0;
	num_real_true = 0
	num_pred_true =0
	num_pred_1 = 0
	num_pred_1_to_1 = 0
	for i in range(len(LP)):
		if LP[i][0] == LP[i][1]:
			num_right +=1;
		if LP[i][0] !=0:
			num_real_true +=1
			if LP[i][1]== 1:
				num_pred_true+=1
		if LP[i][1]:
			num_pred_1 +=1
			if LP[i][0] == 1:
				num_pred_1_to_1  +=1
	print  'the recall = ' + str(float(num_pred_true)/num_real_true)
	print 'the precision = '+ str(float(num_pred_1_to_1)/num_pred_1)
	print 'F1 = ' + str((float(num_pred_true)/num_real_true)*(float(num_pred_1_to_1)/num_pred_1))

sc = SparkContext("local[4]","First Spark App")
user_data = sc.textFile("/home/workingloong/spark_python/pre_deal_data_3.txt")
print user_data.first()
data = user_data.map(lambda line : line.split(","))
print data.first()


user_data_dt = data.map(lambda r : LabeledPoint(extract_label_dt(r),extract_features_dt(r)))
#(trainingData, testData) = user_data_dt.randomSplit([0.7, 0.3])
trainingData = user_data_dt
testData = user_data_dt

RF_model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                    numTrees=60, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=9, maxBins=32)

preds = RF_model.predict(testData.map(lambda p : p.features))
print preds.count()
pred_result = preds.map(lambda r: click_decision(r))

labelsAndPredictions = testData.map(lambda lp: lp.label ).zip(pred_result)
LP = labelsAndPredictions.collect()
evaluate_pred(LP)

# Save and load model
RF_model.save(sc, "RandomForestModelPath_60_9") # numTrees =30, maxDepth = 7
#sameModel = DecisionTreeModel.load(sc, "myModelPath")

preds_print = preds.collect()

f = open('pred_week7_RF_60_7.txt','a')
for i in preds_print:
	f.write(str(i))
	f.write('\r\n')
f.close()


