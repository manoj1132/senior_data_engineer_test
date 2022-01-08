def users_subscriptions_pull():
  import pandas as pd
  import json
  import requests
  
  from pyspark.sql import SparkSession
  import pyspark.sql.functions as F
  
  # create spark session object
  spark = SparkSession.builder.getOrCreate()
  
  # read data from html using requests module
  users_url:'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users'
  response = requests.get(users_url)
  
  if response.status_code != 200:
    print("Below exception occured while calling API.. \n")
    raise ValueError(response.content)
  else:
    data_users = response.json()
    data_users_df = pd.json_normalize(data_users)
 
 # taking subset of dataframe as for users
 data_users_df_subset = data_users_df[['createdAt', 'updatedAt', 'firstName', 'lastName', 'address', 'city', 'country', 'zipcode', 'email', 'birthDate',
                                       'id', 'profile.gender', 'profile.isSmoking', 'profile.profession', 'profile.income']]

 data_users_df_rnm = data_users_df_subset.rename(columns={'createdAt':'created_at',
                                                           'updatedAt':'updated_at', 
                                                          'firstName':'first_name', 
                                                          'lastName':'last_name', 
                                                          'address':'address', 
                                                          'city':'city', 
                                                          'country':'country', 
                                                          'zipcode':'zipcode', 
                                                          'email':'email', 
                                                          'birthDate':'birth_date',
                                                             'id':'id', 
                                                          'profile.gender':'profile_gender', 
                                                          'profile.isSmoking':'profile_issmoking',
                                                          'profile.profession':'profile_profession', 
                                                          'profile.income':'profile_income'
                                                              ,})
 data_users_df_rnm['email'] = data_users_df_rnm['email'].str.split('@').str[1] # to remove user name from email and keeping only email host
 #save to file
 data_users_df_rnm.to_csv('<local_path_to_save>/users.csv', index=False)
 print("Users data write to csv Success")

df_subscription = data_users_fd[['id', 'subscription']]

# creating spark dataframe
s_subscription = spark.creatDataFrame(df_subscription)

s_subscription_explode = s_subscription.select("id", F.explode("subscription")).select("id", "col.endDate", "col.amount", "col.status", "col.createdAt", "col.startDate")

# converting back to pandas dataframe
subscription_df_clean = s_subscription_explode.toPandas()

 subscription_df_clean_rnm = subscription_df_clean.rename(columns={'id':'id',
                                                           'endDate':'end_date', 
                                                          'amount':'amount', 
                                                          'status':'status', 
                                                          'createdAt':'created_at', 
                                                          'startDate':'start_date',})
 #save to file
 subscription_df_clean_rnm.to_csv('<local_path_to_save>/subscription.csv', index=False)
 print("Subscriptions data write to csv Success")

# close spark session object
 spark.close()
