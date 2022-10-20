from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import avg
from pyspark.sql.window import Window

class Walmart_Ecommerce:
    
    def __init__(self,path):
        self.path = path
        spark = SparkSession.builder.master("local[4]").appName("Walmart Ecommerce").getOrCreate()
        self.df = spark.read.json(self.path)
        self.df.toPandas().to_csv('OutputDatas/walmart_ecommerce_product_details.csv',index= False)
    

    
    
    #1. Get the Brand along with products associated with it.
    def Question1(self):
        df =self.df
        brand_with_product = df.select('Brand','Product Name')
        brand_with_product = brand_with_product.distinct()
        brand_with_product.toPandas().to_csv('OutputDatas/1.brand_with_product.csv',index= False)
        return None
    
    
    
    
    
    #2. List all the product names whose list price is greater than sales price
    def Question2(self):
        df  =self.df
        list_price_greater_than_sales_price = df.select('Product Name','List Price','Sale Price')
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.withColumn("List Price",col("List Price").cast("float"))
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.withColumn("Sale Price",col("Sale Price").cast("float"))
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.filter(df['List Price']>df['Sale Price'])
        disttinct_list_price_greater_than_sales_price = list_price_greater_than_sales_price.distinct()
        disttinct_list_price_greater_than_sales_price.toPandas().to_csv('OutputDatas/2.list_price_greater_than_sales_price.csv',index= False)
        return None
    
    
    
    
    
    
    #3. Count the number of product names whose list price is greater than sales price
    def Question3(self):
        df = self.df
        list_price_greater_than_sales_price = df.select('Product Name','List Price','Sale Price')
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.withColumn("List Price",col("List Price").cast("float"))
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.withColumn("Sale Price",col("Sale Price").cast("float"))
        list_price_greater_than_sales_price = list_price_greater_than_sales_price.filter(df['List Price']>df['Sale Price'])
        countt = list_price_greater_than_sales_price.select('Product Name').distinct().count()
        print(f'The number of product names whose list price is greater than sales price is {countt}')
        return None
    
    
    
    
    
    
    #4. List all the products belong to a “women” category.
    
    def Question4(self):
        df = self.df
        products_belongs_women_category = df.where(col('Category').contains('Women') | col('Category').contains('women'))
        products_belongs_women_category = products_belongs_women_category.select('Product Name','Category')
        distinct_products_belongs_women_category = products_belongs_women_category.distinct()
        distinct_products_belongs_women_category.toPandas().to_csv('OutputDatas/4.products_belongs_women_category.csv',index= False)
        return None
    
    
    
    
    
    #5. List the products which are not available.
    def Question5(self):
        df =self.df
        product_not_available = df.filter(df.Available == 'FALSE').select('Available','Product Name')
        product_not_available = product_not_available.distinct()
        product_not_available.toPandas().to_csv('OutputDatas/5.product_not_available.csv',index= False)
        return None
    
    
    
    
    
    #6. Count the number of products which are available.
    def Question6(self):
        df=self.df
        product_available = df.filter(df.Available == 'TRUE').select('Product Name')
        available_product_count = product_available.distinct().count()
        print(f'No. of product which are available is {available_product_count}')
        return None






    #7. List the products that are made up of Nylon.
    def Question7(self):
        df= self.df
        nylon_producs = df.where(col('Description').contains('Nylon') | col('Description').contains('nylon'))
        nylon_producs = nylon_producs.select('Product Name', 'Description')
        distiinct_nylon_products = nylon_producs.distinct()
        distiinct_nylon_products.toPandas().to_csv('OutputDatas/7.nylon_products.csv',index= False)
        return None
    
    
    
    
    

        
    
        
        
    
    
    
    
    
    
if __name__ == "__main__":
    path = "walmart_ecommerce_product_details.json"
    
    walmart = Walmart_Ecommerce(path)

    walmart.Question1()
    walmart.Question2()
    walmart.Question3()
    walmart.Question4()
    walmart.Question5()
    walmart.Question6()
    walmart.Question7()
    
