import os
import sys

os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python2.7'
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell')
os.environ['JAVA_TOOL_OPTIONS'] = "-Dhttps.protocols=TLSv1.2"

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')

from pyspark import SparkContext
from pyspark import HiveContext
from pyspark.sql import  SQLContext
from pyspark.sql import  functions as func
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib


sc = SparkContext()
sqlContext = HiveContext(sc)

#Building the Schema for importing the Youtube Data file
schema_youtube =  StructType([StructField("video_id", StringType(), True),
    StructField("trending_date", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("publish_time", StringType(), True),
    StructField("views", DecimalType(precision=18,scale=2), True),
    StructField("likes", DecimalType(precision=18,scale=2), True),
    StructField("dislikes", DecimalType(precision=18,scale=2), True),
    StructField("comment_count", DecimalType(precision=18,scale=2), True),
    StructField("thumbnail_link", StringType(), True),
    StructField("comments_disabled", StringType(), True),
    StructField("ratings_disabled", StringType(), True),
    StructField("video_error_or_removed", StringType(), True),
    StructField("country", StringType(), True)
  ])


#Importing the Youtube data file using the schema build for analysis
df = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true').schema(schema_youtube)\
    .load('file:///home/cloudera/Downloads/YouTubeData.csv')


#Checking that the data is loaded properly
print("*********************VIEWING THE IMPORTED DATASET****************************")
df.show(n=10)



#Selecting only the columns from the dataframe that we are interested in
#which are video_id ,  title , category_id , views , likes, dislikes, comment_count, country
sqlContext.registerDataFrameAsTable(df,"table1")
youtube_df = sqlContext.sql("SELECT video_id,title,category_id,views,likes,dislikes,comment_count,country from table1")
youtube_df.show()
youtube_df.registerTempTable('DataTable')

youtube_df1 = sqlContext.sql("SELECT * FROM DataTable")

print("*********************VIEWING YOUTUBE DATASET****************************")
youtube_df1.show(n=10)



#Importing the Youtube Category details from the text file
youtube_categories = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true') \
     .load('file:///home/cloudera/Downloads/Categories.csv')


#Checking the Schema of the categories file to make sure datatypes are correct
print("*********************VIEWING CATEGORIES SCHEMA****************************")
youtube_categories.printSchema()


#Renaming the column names of the Categories data
sqlContext.registerDataFrameAsTable(youtube_categories,"table2")
categories = sqlContext.sql("SELECT Category_ID as category_id, Category as category_name from table2")
print("*********************VIEWING CATEGORIES DATASET****************************")
categories.show(n=5) #Checking that the data is loaded properly
categories.registerTempTable('CategoryTable')


#Joining the Youtube data with Categories to get the corresponding Category Name

##################################This is our final dataset########################################
df = sqlContext.sql("SELECT DataTable.*,CategoryTable.category_name from DataTable,CategoryTable where DataTable.category_id = CategoryTable.category_id")

print("*********************VIEWING YOUTUBE FINAL DATASET****************************")
df.show(n=10)
df.registerTempTable('YoutubeData')



############################EXPLORATORY ANALYSIS OF YOUTUBE DATASET#################################


########################################Categories################################################
###################Top 10 categories with maximum number of videos uploaded#######################
df1 = df
sqlContext.registerDataFrameAsTable(df1,"Top10Categories")
print("****************************TOP 10 CATEGORIES WITH MAXIMUM VIDEOS*****************************")
ans1 =  sqlContext.sql("SELECT category_name ,COUNT(category_id) as category_count  from Top10Categories  group by category_id, category_name order by category_count DESC")
ans1.show(n=10)




###################Top 10 most commented categories#####################
df2 = df.orderBy('category_name').groupBy('category_name').sum('comment_count')
sqlContext.registerDataFrameAsTable(df2,"CategoryComment")
print("****************************TOP 10 MOST COMMENTED CATEGORIES*****************************")
ans2 =  sqlContext.sql("SELECT * from CategoryComment")
ans2 = ans2.orderBy('sum(comment_count)', ascending= False)
ans2.show(n=10)




###################Top 10 most viewed categories#######################
df3 = df.orderBy('category_name').groupBy('category_name').sum('views')
sqlContext.registerDataFrameAsTable(df3,"CategoryViews")
print("****************************TOP 10 MOST VIEWED CATEGORIES*****************************")
ans3 =  sqlContext.sql("SELECT * from CategoryViews")
ans3 = ans3.orderBy('sum(views)', ascending= False)
ans3.show(n=10)





###################Top 10 most liked categories#######################
df4 = df.orderBy('category_name').groupBy('category_name').sum('likes')
sqlContext.registerDataFrameAsTable(df4,"CategoryLikes")
print("****************************TOP 10 MOST LIKED CATEGORIES*****************************")
ans4 =  sqlContext.sql("SELECT * from CategoryLikes")
ans4 = ans4.orderBy('sum(likes)', ascending= False)
ans4.show(n=10)



###################Top 10 most disliked categories#######################
df5 = df.orderBy('category_name').groupBy('category_name').sum('dislikes')
sqlContext.registerDataFrameAsTable(df5,"CategoryDislikes")
print("****************************TOP 10 MOST DISLIKED CATEGORIES*****************************")
ans5 = sqlContext.sql("SELECT * from CategoryDislikes")
ans5 = ans5.orderBy('sum(dislikes)', ascending= False)
ans5.show(n=10)



########################################Videos################################################

#Since there are same file being uploaded multiple times across various channels, also having different trending dates,
#We will take the top most file in each video_id group according to our requirements
#Hence Partitioning of the table based on video_id and Ranking is done for this purpose

df_video = df.orderBy('video_id')
sqlContext.registerDataFrameAsTable(df_video,"YoutubeData") #This will be the datatable which will be used for analysis throughout

##################Top 10 videos with largest number of Views######################################
print("****************************TOP 10 VIDEOS WITH MAXIMUM VIEWS*****************************")
ans6 = sqlContext.sql("with cte as (select *, ROW_NUMBER() over (PARTITION BY video_id ORDER BY views DESC) as rn FROM  YoutubeData) select video_id,title,views from cte where rn=1 order by views desc")
ans6.show(n=10)


#########3###############Top 10 liked videos in Youtube############################################
print("****************************TOP 10 LIKED VIDEOS*****************************")
ans7 = sqlContext.sql("with cte as (select *, ROW_NUMBER() over (PARTITION BY video_id ORDER BY likes DESC) as rn FROM  YoutubeData) select video_id,title,likes from cte where rn=1 order by likes desc")
ans7.show(n=10)




########################Top 10 disliked videos in Youtube########################################
print("****************************TOP 10 DISLIKED VIDEOS*****************************")
ans8 = sqlContext.sql("with cte as (select *, ROW_NUMBER() over (PARTITION BY video_id ORDER BY dislikes DESC) as rn FROM  YoutubeData) select video_id,title,dislikes from cte where rn=1 order by dislikes desc")
ans8.show(n=10)



########################Top 10 most commented videos in Youtube####################################
print("****************************TOP 10 COMMENTED VIDEOS*****************************")
ans9 = sqlContext.sql("with cte as (select *, ROW_NUMBER() over (PARTITION BY video_id ORDER BY comment_count DESC) as rn FROM  YoutubeData) select video_id,title,comment_count from cte where rn=1 order by comment_count desc")
ans9.show(n=10)



#Saving the output in files

output_path = "file:///home/cloudera/Desktop/Output/"

#****************************TOP 10 CATEGORIES WITH MAXIMUM VIDEO UPLOAD*****************************
ans1.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer1_category_upload.csv")


#****************************TOP 10 MOST COMMENTED CATEGORIES*****************************
ans2.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer2_category_comment.csv")


#****************************TOP 10 MOST VIEWED CATEGORIES*****************************
ans3.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer3_category_view.csv")


#****************************TOP 10 MOST LIKED CATEGORIES*****************************
ans4.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path+ "Answer4_category_liked.csv")


#****************************TOP 10 MOST DISLIKED CATEGORIES*****************************
ans5.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer5_category_disliked.csv")



#****************************TOP 10 VIDEOS WITH MAXIMUM VIEWS*****************************
ans6.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer6_video_view.csv")

#****************************TOP 10 LIKED VIDEOS*****************************
ans7.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path+ "Answer7_video_liked.csv")


#****************************TOP 10 DISLIKED VIDEOS****************************
ans8.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path +"Answer8_video_disliked.csv")


#****************************TOP 10 COMMENTED VIDEOS*****************************
ans9.coalesce(1).write.format('com.databricks.spark.csv') \
   .option('header', 'true') \
  .save(output_path + "Answer9_video_comments.csv")




