'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os
import time



spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions') #'output/questions-transformed'

print(questions_input_path)

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

# Start time
start_time = time.time()
'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF= questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print(resultDF.explain(True))

# End time
end_time = time.time()
print(f"Execution Time for First Query: {end_time - start_time:.2f} seconds")

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
''' 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

# Assuming Spark session is initialized
spark = SparkSession.builder.appName('Optimize Query').getOrCreate()


# Repeat for the second query
start_time = time.time()

# Repartition DataFrames to minimize shuffle during join
answersDF = answersDF.repartition('question_id')
questionsDF = questionsDF.repartition('question_id')

# Answers aggregation: Get number of answers per question per month
# Apply filtering and select only necessary columns before aggregation
answers_month = (answersDF
                 .withColumn('month', month('creation_date'))
                 .groupBy('question_id', 'month')
                 .agg(count('*').alias('cnt')))

# Perform the join and select only the necessary columns
resultDF2= (questionsDF
            .join(answers_month, 'question_id')
            .select('question_id', 'creation_date', 'title', 'month', 'cnt'))

# Order by question_id and month
resultDF2= resultDF2.orderBy('question_id', 'month')

# Show results
resultDF2.show()

print(resultDF2.explain(True))
end_time = time.time()
print(f"Execution Time for Second Query: {end_time - start_time:.2f} seconds")
