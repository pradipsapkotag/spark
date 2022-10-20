from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import avg
from pyspark.sql.window import Window

class Udemy_courses:
    
    def __init__(self,path):
        self.path = path
        spark = SparkSession.builder.master("local[4]").appName("Udemy Courses").getOrCreate()
        self.df = spark.read.json(self.path)
        self.df.toPandas().to_csv('OutputDatas/udemy_courses.csv',index= False)
    

    
    
    # 1. What are the best free courses by subject?
    def Question1(self):
        df =self.df
        free_courses= df.filter(df.is_paid == "False")
        free_courses=free_courses.withColumn("num_reviews",col("num_reviews").cast("integer"))
        free_courses= free_courses.withColumn("num_subscribers",col("num_subscribers").cast("integer"))
        best_free_courses = free_courses.orderBy(free_courses.subject,free_courses.num_subscribers.desc(),free_courses.num_reviews.desc())
        best_free_courses_by_subject=best_free_courses.select(best_free_courses.course_title,best_free_courses.subject,best_free_courses.num_subscribers)
        best_free_courses_by_subject.toPandas().to_csv('OutputDatas/1.Best_free_courses_by_subject.csv',index= False)
        return None
    
    
    
    
    
    #2.What are the most popular courses?
    def Question2(self):
        most_popular  =self.df
        most_popular=most_popular.withColumn("num_reviews",col("num_reviews").cast("integer"))
        most_popular= most_popular.withColumn("num_subscribers",col("num_subscribers").cast("integer"))
        most_popular = most_popular.orderBy(most_popular.num_subscribers.desc())
        most_popular_courses = most_popular.select(most_popular.course_title,most_popular.num_subscribers)
        most_popular_courses.toPandas().to_csv('OutputDatas/2.Most_popular_courses.csv',index= False)
        return None
    
    
    
    
    
    
    #3. List the courses that are specialized to “Business Finance” and find the average number of subscribers, reviews, price and lectures on the subject.
    def Question3(self):
        df = self.df
        business_finance =  df.filter(df.subject == "Business Finance")
        business_finance = business_finance.withColumn("num_reviews",col("num_reviews").cast("integer"))
        business_finance = business_finance.withColumn("num_subscribers",col("num_subscribers").cast("integer"))
        business_finance = business_finance.withColumn("num_lectures",col("num_lectures").cast("double"))
        business_finance = business_finance.withColumn("price",col("price").cast("double"))
        business_finance.toPandas().to_csv('OutputDatas/3.Business_Finance.csv',index= False)
        avgbusiness_finance = business_finance.agg({'price': 'mean','num_lectures': 'mean','num_reviews': 'mean','num_subscribers' : 'mean'})
        avgbusiness_finance.toPandas().to_csv('OutputDatas/3.Business_Finance_avg.csv',index= False)
        return None
    
    
    
    
    
    
    #4. How are courses related?
    
    ## correlation is not calculated
    
    
    
    
    
    #5. Which courses offer the best cost benefit?
    def Question5(self):
        df =self.df
        best_cost_benefit = df.select('course_title','price','content_duration')
        best_cost_benefit = best_cost_benefit.withColumn("price",col("price").cast("float")).withColumn("content_duration",col("content_duration").cast("float"))
        best_cost_benefit = best_cost_benefit.withColumn('content_dutation_minutes',col('content_duration')*60)\
                                        .withColumn('duration_minPerPrice',col('content_dutation_minutes')/col('price'))
        best_cost_benefit= best_cost_benefit.orderBy(best_cost_benefit.duration_minPerPrice.desc())
        distinct_best_cost_benefit = best_cost_benefit.distinct().orderBy(best_cost_benefit.duration_minPerPrice.desc())
        distinct_best_cost_benefit.toPandas().to_csv('OutputDatas/5.Best_cost_benefit.csv',index= False)
        return None
    
    
    
    
    
    #6. Find the courses which have more than 15 lectures.
    def Question6(self):
        df=self.df
        more_than_15_lectures = df.select(df.course_title,df.num_lectures)
        more_than_15_lectures = more_than_15_lectures.withColumn("num_lectures",col("num_lectures").cast("double"))
        more_than_15_lectures = more_than_15_lectures.filter(more_than_15_lectures.num_lectures>15)
        more_than_15_lectures.toPandas().to_csv('OutputDatas/6.More_than_15_lectures.csv',index= False)
        return None






    #7. List courses on the basis of level.
    def Question7(self):
        df= self.df
        level_basis = df.orderBy(df.level)
        level_basis= level_basis.dropna(subset =['subject'])
        level_basis = level_basis.select(level_basis.course_title,level_basis.level)
        level_basis.toPandas().to_csv('OutputDatas/7.level_basis_courses.csv',index= False)
        return None
    
    
    
    
    
    #8. Find the courses which have duration greater than 2 hours.
    def Question8(self):
        df=self.df
        greater_than_2hours = df.select(df.course_title,df.content_duration)
        greater_than_2hours = greater_than_2hours.withColumn("content_duration",col("content_duration").cast("float"))
        greater_than_2hours= greater_than_2hours.filter(greater_than_2hours.content_duration> 2.0)
        greater_than_2hours.toPandas().to_csv('OutputDatas/8.greater_than_2hours.csv',index= False)
        return None
        
    
        
        
    
    
    
    
    
    
if __name__ == "__main__":
    path = "udemy_courses.json"
    
    udemy = Udemy_courses(path)

    udemy.Question1()
    udemy.Question2()
    udemy.Question3()
    udemy.Question5()
    udemy.Question6()
    udemy.Question7()
    udemy.Question8()
    
