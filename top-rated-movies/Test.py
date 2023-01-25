#Pyspark code for top rated movies 
#input (user_id,movie_id,rating_given,timestamp)
#problem - find out for each star how much movies are there 

from pyspark import SparkContext

sc=SparkContext("local","top-rated-movies")

rdd1=sc.textFile("D:\workspace/pyspark-pratice-problems/top-rated-movies/moviedata-201008-180523.data")

rdd2=rdd1.map(lambda x : x.split(",")[2])

rdd3=rdd2.map(lambda x: (x,1))

rdd4=rdd3.reduceByKey(lambda x,y : x+y)

print("count is ", rdd4.sortByKey().collect())