#Pyspark code to get top customer 
#input - customerID, productID, amount
#output- top customer who order most

from pyspark import SparkContext

sc=SparkContext("local","top-customer")

#read the csv file
rdd1=sc.textFile("D:\workspace/top-customer/customerorders-201008-180523.csv")

#take two columns only and convert them to int and float
rdd2=rdd1.map(lambda x: (int(x.split(",")[0]), float(x.split(",")[2])))

#DO aggreagtion on key and sort 
rdd3=rdd2.reduceByKey(lambda x,y : (x+y)).sortBy(lambda x: x[1], False)

print("Top customers with there amount ",rdd3.collect())