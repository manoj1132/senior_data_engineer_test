def messages_pull():
  import pandas as pd
  import json
  import requests
  
  messages_url:'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages'
  response = requests.get(messages_url)
  
  if response.status_code != 200:
    print("Below exception occured while calling API.. \n")
    raise ValueError(response.content)
  else:
    data_messages = response.json()
    data_messages_df = pd.json_normalize(data_messages)
 
 # taking subset of dataframe as messages should not be imported
 data_messages_df_subset = data_messages_df[['createdAt', 'receiverId', 'id', 'senderId']]
 data_messages_df_rnm = data_messages_df_subset.rename(columns={'createdAt':'created_at',
                                                                'receiverId':'receiver_id',
                                                                'id':'id',
                                                                'senderId':'sender_id',})
 #save to file
 data_messages_df_rnm.to_csv('<local_path_to_save>/messages.csv', index=False)
 
 print("Messages data write to csv Success")
