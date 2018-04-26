from pyspark import SparkContext
sc = SparkContext()
#import sequence file from hdfs 
data = sc.sequenceFile("flume/events")
type(data)
data.collect()

#remove the key from the data, lambda is a nameless function taking x as variable for the function after the colon, map applies the fuction to all the objects in the rdd

data_rdd_one = data.map(lambda x:x[1])

#Converting the rdd to list
data_list = data_rdd_one.collect()
type(data_list)
data_list[0]

#strip the columns 
data_list.pop(0)

#splitting the elements in the list
for i in range(len(data_list)):
	x = data_list[i]
	data_list[i] = x.split("w")
	for j in range(len(data_list[i])):
		data_list[i][j] = float(data_list[i][j])
		



#Creating the dataframe using the list: data_list
from pyspark.sql.types import *

schema = StructType([StructField("Index",DoubleType(), True),StructField("APC",DoubleType(), True),StructField("BP",DoubleType(), True),StructField("COP",DoubleType(), True),StructField("CVX",DoubleType(), True),StructField("HES",DoubleType(), True),StructField("MRO",DoubleType(), True),StructField("OXY",DoubleType(), True),StructField("PBR",DoubleType(), True),StructField("TOT",DoubleType(), True),StructField("VLO",DoubleType(), True),StructField("XOM",DoubleType(), True),StructField("CL",DoubleType(), True),StructField("USO",DoubleType(), True)]) 
data_df = sqlContext.createDataFrame(data_list,schema = schema)
data_df = data_df.select(['APC','BP','COP','CVX','HES','MRO','OXY','PBR','TOT','VLO','XOM','CL','USO'] )

#Machine Learning(Linear Regression)
from pyspark.ml import *
from pyspark.mllib.linalg import DenseVector
input_data = data_df.rdd.map(lambda x: (x[12], DenseVector(x[:-1])))
df = sqlContext.createDataFrame(input_data, ["label", "features"])
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol = "label", maxIter = 1000, regParam = 0.3, elasticNetParam = 0.8)
train_data, test_data = df.randomSplit([.8,.2],seed =1234)
linearModel = lr.fit(train_data)

#Prediction
predicted = linearModel.transform(test_data)
predicted.count()

#Coefficients
linearModel.coefficients
linearModel.intercept

#Evaluation of the model
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol = "prediction")
evaluator.evaluate(predicted)
evaluator.evaluate(predicted,{evaluator.metricName:"r2"})
evaluator.evaluate(predicted,{evaluator.metricName:"mae"})
