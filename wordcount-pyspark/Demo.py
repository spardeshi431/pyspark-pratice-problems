#Pyspark code for wordcount

from pyspark import SparkContext
from time import sleep
sc=SparkContext("local", "Wordcount")

#Read the file
rdd1=sc.textFile("D:\workspace\wordcount-pyspark/data.txt")


rdd2=rdd1.flatMap(lambda x : x.split(" "))

rdd3=rdd2.map(lambda x : x.upper())

rdd4=rdd3.map(lambda x : (x,1))

rdd5=rdd4.reduceByKey(lambda x,y : x+y)


print("Before collect " , rdd5.count())
list1=rdd5.sortBy(lambda x : x[1], False).collect()
print("after collect ",  list1)

#Sleep added just to check the dag
#Check DAG on localhost:4040
sleep(100)



