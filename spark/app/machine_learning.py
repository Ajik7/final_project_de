import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import numpy as np
from sklearn.impute import SimpleImputer 
from sklearn import * 
import time   
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_absolute_error

# Create spark session
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

#parameter
postgre_server = "localhost"
postgre_port = "5432"
postgre_user = "postgres"
postgre_password = "postgres"
postgre_url = f"jdbc:postgresql://localhost:5432/postgres"

#read data from postgre
app_train = (
     spark.read
    .format("jdbc")
    .option("server", postgre_server)
    .option("url", postgre_url)
    .option("dbtable", "application_train")
    .option("user", postgre_user)
    .option("password", postgre_password)
    .load()
    )

app_test = (
     spark.read
    .format("jdbc")
    .option("server", postgre_server)
    .option("url", postgre_url)
    .option("dbtable", "application_test")
    .option("user", postgre_user)
    .option("password", postgre_password)
    .load()
    )

#convert to dataframe
data_train = app_train.toPandas()
data_test = app_test.toPandas()

#set target column, 0 ay on time, 1 dufficult payment
target = data_train['TARGET']

#one hot encoding for categorical data
def one_hot_encoding(data_train, data_test):
    encoded_data_train = pd.get_dummies(data_train)
    encoded_data_test = pd.get_dummies(data_test)
    return encoded_data_train, encoded_data_test

encoded_data_train, encoded_data_test = one_hot_encoding(data_train, data_test)

#align jumlah kolom data train dan data test
align_data_train, align_data_test = encoded_data_train.align(encoded_data_test, join = 'inner', axis = 1)
align_data_train['TARGET'] = target

#handle missing value
align_data_train = align_data_train.drop(align_data_train.columns[align_data_train.isnull().mean()>0.60], axis = 1)
align_data_test = align_data_test.drop(align_data_test.columns[align_data_test.isnull().mean()>0.60], axis = 1)

#handle negative value
col_with_mean = align_data_train.mean(axis = 0)
col_name_with_negative_mean = ['DAYS_BIRTH','DAYS_REGISTRATION','DAYS_ID_PUBLISH','DAYS_LAST_PHONE_CHANGE']
align_data_train[col_name_with_negative_mean] = align_data_train[col_name_with_negative_mean]/-365
align_data_test[col_name_with_negative_mean] = align_data_test[col_name_with_negative_mean]/-365
align_data_train['DAYS_EMPLOYED'].replace({365243 : np.nan}, inplace=True)
align_data_test['DAYS_EMPLOYED'].replace({365243 : np.nan}, inplace=True)
align_data_train['DAYS_EMPLOYED'] = abs(align_data_train['DAYS_EMPLOYED'])
align_data_test['DAYS_EMPLOYED'] = abs(align_data_test['DAYS_EMPLOYED'])

#train test data modelling
column_train = align_data_train.drop(columns=["SK_ID_CURR","TARGET"])
column_test = align_data_test.drop(columns=["SK_ID_CURR"])
## modelling
X = column_train
y = align_data_train["TARGET"]
X_test = column_test

#impute null value with 
def impute_missing_values(data_train, data_test):  
    imputer = SimpleImputer(missing_values=np.nan, strategy='median')
    imputed_data_train = imputer.fit_transform(data_train)
    imputed_data_test = imputer.transform(data_test)
    return imputed_data_train, imputed_data_test

X, X_test = impute_missing_values(X, X_test)

# logistic regression model
def create_logictic_regression_model(X, y):
    
    print('Starting Logistic regression model training...')
    t0 = time.time()
    
    model = LogisticRegression()
    model.fit(X,y)

    t1 = time.time()
    print('Time elapsed during logistic regression model training is : ', t1-t0)
    
    return model
logistic_regr_model = create_logictic_regression_model(X,y)
logistic_regr_pred = logistic_regr_model.predict_proba(X_test)[:,1]

submit = align_data_test[['SK_ID_CURR']]
submit['TARGET'] = logistic_regr_pred

#load to postgre
(
    submit.write
    .format("jdbc")
    .option("server", postgre_server)
    .option("url", postgre_url)
    .option("dbtable", "home_credit_default_risk_application_ml_result")
    .option("user", postgre_user)
    .option("password", postgre_password)
    .save()
)
