import pyspark
from pyspark.sql import SparkSession

# Create spark session
if __name__ == "__main__":
    # initiate spark
    try:
        spark = SparkSession.builder\
            .master("local")\
            .config("spark.jars","/usr/local/spark/resources/mysql-connector-java-8.0.29.jar")\
            .appName("MySql").getOrCreate()\
            
            
    except Exception as e:
        print("LOG ERROR SPARK SESSION")
        print(f"{e}")
#Parameter
mysql_driver = "com.mysql.cj.jdbc.Driver"
mysql_url = f"jdbc:mysql://localhost:3306/mysql"
mysql_user = "root"
mysql_password="mysql"


#Read CSV
df_train = (
    spark.read
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", "true")\
    .load("/usr/local/spark/resources/application_train.csv")
)

df_test = (
    spark.read
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", "true")\
    .load("/usr/local/spark/resources/application_test.csv")
)

# Load data to Mysql
(
    df_train.write
    .format("jdbc")\
    .option("driver", mysql_driver)\
    .option("url", mysql_url)\
    .option("dbtable", "application_train")\
    .option("user", mysql_user)\
    .option("password", mysql_password)\
    .save()
)
(
    df_test.write
    .format("jdbc")\
    .option("driver", mysql_driver)\
    .option("url", mysql_url)\
    .option("dbtable", "application_test")\
    .option("user", mysql_user)\
    .option("password", mysql_password)\
    .save()
)
