# import libraries
import pandas as pd
from googleapiclient.discovery import build
import pymysql
import pymongo
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from streamlit_option_menu import option_menu

st.set_page_config(page_title = "Youtube Data Harvesting",
                   page_icon= "youtube",
                   layout = "wide")

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

#API connection
def api_Connection():
    
    api_key = 'AIzaSyD1kSkZAhkpaqZvCKUZ_x3Aiz1IR_MzQVc'
    api_service_name = 'youtube'
    api_version = 'v3'

    youtube=build(api_service_name,api_version,developerKey=api_key)
    
    return youtube

youtube = api_Connection()

#To get Channel Information
def get_channel_Info(channel_id):
    
  channel_data=[]
  
  request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id)
  
  response = request.execute()

  for i in response['items']:
    data = dict(Channel_name = i['snippet']['title'],
                Channel_Id = i['id'],
                Channel_Subscribers = i['statistics']['subscriberCount'],
                Channel_Views = i['statistics']['viewCount'],
                Channel_Total_Videos = i['statistics']['videoCount'],
                Channel_Description = i['snippet']['description'],
                Channel_playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])
    
  
    channel_data.append(data)
    
  return channel_data

#To Get Channel_Video_id
def get_video_ids(channel_id):
  
  video_ids = []
  
  response = youtube.channels().list(part="contentDetails",id=channel_id).execute()
  
  Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  
  more_page=True
  next_page_token=None
  
  while more_page:
       
    request=youtube.playlistItems().list(
              part='snippet',
              playlistId=Playlist_id,
              maxResults=60,
              pageToken=next_page_token)
    response=request.execute()

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

    next_page_token = response.get('nextPageToken')

    if next_page_token is None:
      more_page = False

  return video_ids

#To Get Channel_Video_id
def get_video_details(Video_Ids):
  all_videos_data = []

  for i in range(0,len(Video_Ids),50):
    request = youtube.videos().list(
                part = 'snippet,contentDetails,statistics',
                id=','.join(Video_Ids[i:i+50]))

    response = request.execute()

    for video in response['items']:
      video_stats = dict(Channel_Name=video['snippet']['channelTitle'],
                         Channel_Id=video['snippet']['channelId'],
                         Video_Id =video['id'],
                         Video_Title = video['snippet']['title'],
                         Video_Tags = video['snippet'].get('tags'),
                         Video_Description = video['snippet'].get('description'),
                         Video_Publisheddate = video['snippet']['publishedAt'],
                         Video_Thumbnails = video['snippet']['thumbnails']['default']['url'],
                         Video_Duration = video['contentDetails']['duration'],
                         Video_Definition = video['contentDetails']['definition'],
                         Video_Caption = video['contentDetails']['caption'],
                         Video_Views = video['statistics']['viewCount'],
                         Video_Favouritecount = video['statistics'].get('favoriteCount'),
                         Video_Likes = video['statistics'].get('likeCount'),
                         Video_Comments = video['statistics'].get('commentCount'))
      all_videos_data.append(video_stats)

  return all_videos_data

#To Get Comment Details
def get_comment_details(Video_Ids):
  
  all_comment_data = []
  try:
    for video_id in Video_Ids:
      request = youtube.commentThreads().list(
              part='snippet',
              videoId=video_id,
              maxResults=50)

      response = request.execute()

      for comment in response['items']:
        Comment_data = dict(Comment_Id=comment['snippet']['topLevelComment']['id'],
                            Video_Id=comment['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt=comment['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
        all_comment_data.append(Comment_data)
    
  except:
    pass

  return all_comment_data

#To Get Playlist Details
def get_playlist_details(channel_id):
  
    playlist_details = []

    more_page = True
    next_page_token = None
    
    while more_page:
  
      request=youtube.playlists().list(
                  part='snippet,contentDetails',
                  channelId=channel_id,
                  maxResults=50,
                  pageToken=next_page_token)
      response=request.execute()
      
      for playlist in response['items']:
        playlist_data = dict(Playlist_Id=playlist['id'],
                            Playlist_title=playlist['snippet']['title'],
                            Channel_Id=playlist['snippet']['channelId'],
                            Channel_name=playlist['snippet']['channelTitle'],
                            Playlist_PublishedAt=playlist['snippet']['publishedAt'],
                            Playlist_TotalVideos=playlist['contentDetails']['itemCount'] 
                            )
        playlist_details.append(playlist_data)
            
      next_page_token = response.get('nextPageToken')
      
      if next_page_token is None:
          more_page = False 

    return playlist_details

#To Load data into MongoDB
def channel_details(channel_id):
    client=pymongo.MongoClient("mongodb+srv://akshaya08:Abhi1%40aks@cluster0.s7e4jmk.mongodb.net/")
    db=client["Youtube_data"]
    
    Channel_Data=get_channel_Info(channel_id) 
    Video_Ids=get_video_ids(channel_id)
    Video_Details=get_video_details(Video_Ids)
    Comment_details=get_comment_details(Video_Ids)
    Playlist_Details=get_playlist_details(channel_id)

    collection=db["Channel_Data"]
    collection.insert_one({"Channel_Details":Channel_Data,"Playlist_Details":Playlist_Details,
                      "Video_Details":Video_Details,"Comment_details":Comment_details})
    
    return "upload completed successfully"

#To Connect with SQL Database
def sql_connect():
    mydb=pymysql.connect(host="qn66usrj1lwdk1cc.cbetxkdyhwsb.us-east-1.rds.amazonaws.com",
                    user="c7vxy08pcs9fuvum",
                    password="v10q2u5vkxnoyen3",
                    database="ita2g6ato5l2k3my",
                    port=3306)

    cursor=mydb.cursor()
    
    return cursor,mydb

#Table creation and migrating selected channel data into SQL Table from MongoDB - channel_data 
def Channel_Data_sqltable(new_channel_data):
    
    cursor,mydb = sql_connect()
    create_query =  '''
        CREATE TABLE IF NOT EXISTS Channel_data (
            Channel_name VARCHAR(100) NOT NULL, 
            Channel_Id VARCHAR(100) PRIMARY KEY,  
            Channel_Subscribers BIGINT DEFAULT 0,  
            Channel_Views BIGINT DEFAULT 0,      
            Channel_Total_Videos INT DEFAULT 0,   
            Channel_Description TEXT,             
            Channel_playlist_Id VARCHAR(100)    
        ) '''
        
    cursor.execute(create_query)
    mydb.commit()
        
    Channel_Data=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({'Channel_Details.Channel_name':new_channel_data},{"_id":0}):
        Channel_Data.append(channel_data['Channel_Details'])
        
    df_Channel_Data = pd.DataFrame(Channel_Data[0])

    for index,row in df_Channel_Data.iterrows():
        insert_query =  '''
                INSERT INTO Channel_data (
                    Channel_name, 
                    Channel_Id,  
                    Channel_Subscribers,  
                    Channel_Views,      
                    Channel_Total_Videos, 
                    Channel_Description,             
                    Channel_playlist_Id)
                    
                    VALUES(%s,%s,%s,%s,%s,%s,%s) ''' 
        values=(row['Channel_name'],
                row['Channel_Id'],
                row['Channel_Subscribers'],
                row['Channel_Views'],
                row['Channel_Total_Videos'],
                row['Channel_Description'],
                row['Channel_playlist_Id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
            return True

        except:
            print("Channel Details already exists")
            return False

#Table creation and migrating selected channel data into SQL Table from MongoDB - video_data
def Video_Details_sqltable(new_channel_data):
    
    cursor,mydb = sql_connect()

    create_query =  '''
            CREATE TABLE IF NOT EXISTS video_data (
                Channel_Name VARCHAR(100) NOT NULL, 
                Channel_Id VARCHAR(100), 
                Video_Id VARCHAR(30) PRIMARY KEY,
                Video_Title VARCHAR(150) NOT NULL,  
                Video_Tags VARCHAR(600),
                Video_Description TEXT,   
                Video_Publisheddate DATETIME,             
                Video_Thumbnails VARCHAR(200),
                Video_Duration TIME,
                Video_Definition VARCHAR(10),
                Video_Caption VARCHAR(50),
                Video_Views BIGINT DEFAULT 0,
                Video_Favouritecount BIGINT DEFAULT 0,
                Video_Likes BIGINT DEFAULT 0,
                Video_Comments BIGINT DEFAULT 0
            ) '''
            
    cursor.execute(create_query)
    mydb.commit()
        
    Video_Details=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({'Channel_Details.Channel_name':new_channel_data},{"_id":0}):
        Video_Details.append(channel_data['Video_Details'])
        
    df_Video_Details = pd.DataFrame(Video_Details[0])
    df_Video_Details['Video_Publisheddate'] = pd.to_datetime(df_Video_Details['Video_Publisheddate'], format='ISO8601')
    df_Video_Details['Video_Duration'] = pd.to_timedelta(df_Video_Details['Video_Duration'])
    # Convert timedelta to string and split by space
    df_Video_Details['Video_Duration'] = df_Video_Details['Video_Duration'].astype(str).str.strip().str.split()
    # Extract the time part (hours:minutes:seconds)
    df_Video_Details['Video_Duration'] = df_Video_Details['Video_Duration'].apply(lambda x: x[-1])
    df_Video_Details['Video_Tags'] = df_Video_Details['Video_Tags'].astype(str)

    for index,row in df_Video_Details.iterrows():
        insert_query =  '''
                INSERT INTO video_data (
                    Channel_name, 
                    Channel_Id, 
                    Video_Id,
                    Video_Title,  
                    Video_Tags,
                    Video_Description,   
                    Video_Publisheddate,             
                    Video_Thumbnails,
                    Video_Duration,
                    Video_Definition,
                    Video_Caption,
                    Video_Views,
                    Video_Favouritecount,
                    Video_Likes,
                    Video_Comments)
                    
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ''' 
                    
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Video_Title'],
                row['Video_Tags'],
                row['Video_Description'],
                row['Video_Publisheddate'],
                row['Video_Thumbnails'],
                row['Video_Duration'],
                row['Video_Definition'],
                row['Video_Caption'],
                row['Video_Views'],
                row['Video_Favouritecount'],
                row['Video_Likes'],
                row['Video_Comments'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()


        except:
            print("Video Details already exists")


        
#Table creation and migrating selected channel data into SQL Table from MongoDB - comment_data
def Comment_Details_sqltable(new_channel_data):

    cursor,mydb = sql_connect()

    create_query =  '''
                CREATE TABLE IF NOT EXISTS comment_data (
                    Comment_Id VARCHAR(100) PRIMARY KEY, 
                    Video_Id VARCHAR(30) NOT NULL, 
                    Comment_Text TEXT,
                    Comment_Author VARCHAR(50) NOT NULL,  
                    Comment_PublishedAt DATETIME) '''
                
    cursor.execute(create_query)
    mydb.commit()
        
    Comment_details=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({'Channel_Details.Channel_name':new_channel_data},{"_id":0}):
        Comment_details.append(channel_data['Comment_details'])
    
    df_Comment_details= pd.DataFrame(Comment_details[0])
    df_Comment_details['Comment_PublishedAt'] = pd.to_datetime(df_Comment_details['Comment_PublishedAt'], format='ISO8601')
        
    for index,row in df_Comment_details.head(100).iterrows():
        insert_query =  '''
                INSERT INTO comment_data (
                    Comment_Id, 
                    Video_Id, 
                    Comment_Text,
                    Comment_Author,  
                    Comment_PublishedAt)
                    
                    VALUES(%s,%s,%s,%s,%s) ''' 
                    
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()


        except:
            print("Comment Details already exists")
            
#Table creation and migrating selected channel data into SQL Table from MongoDB - playlist_data
def Playlist_Details_sqltable(new_channel_data):

    cursor,mydb = sql_connect()

    create_query =  '''
            CREATE TABLE IF NOT EXISTS playlist_data (
                Playlist_Id VARCHAR(50) PRIMARY KEY, 
                Playlist_title VARCHAR(150) NOT NULL, 
                Channel_Id VARCHAR(100) NOT NULL,
                Channel_name VARCHAR(100) NOT NULL,  
                Playlist_PublishedAt DATETIME,
                Playlist_TotalVideos BIGINT DEFAULT 0) '''
            
    cursor.execute(create_query)
    mydb.commit()
    
    Playlist_Details=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({'Channel_Details.Channel_name':new_channel_data},{"_id":0}):
        Playlist_Details.append(channel_data['Playlist_Details'])
        
    df_Playlist_Details = pd.DataFrame(Playlist_Details[0])
    df_Playlist_Details['Playlist_PublishedAt'] = pd.to_datetime(df_Playlist_Details['Playlist_PublishedAt'], format='ISO8601')

    for index,row in df_Playlist_Details.iterrows():
        insert_query =  '''
                INSERT INTO playlist_data (
                    Playlist_Id, 
                    Playlist_title, 
                    Channel_Id,
                    Channel_name,  
                    Playlist_PublishedAt,
                    Playlist_TotalVideos)
                    
                    VALUES(%s,%s,%s,%s,%s,%s) ''' 
                    
        values=(row['Playlist_Id'],
                row['Playlist_title'],
                row['Channel_Id'],
                row['Channel_name'],
                row['Playlist_PublishedAt'],
                row['Playlist_TotalVideos'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()


        except:
            print("Playlist Details already Exists")
                
# Function to Migrate to SQL for all tables
def tables(new_channel_name):

    status1=Channel_Data_sqltable(new_channel_name)

    if status1 == True:
        Video_Details_sqltable(new_channel_name)
        Playlist_Details_sqltable(new_channel_name)
        Comment_Details_sqltable(new_channel_name)
        return True
    else:
        return False

#To show Channel detail in streamlit
def show_channel_detail():
    Channel_Data=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({},{"_id":0,"Channel_Details":1}):
        for i in range(len(channel_data['Channel_Details'])):
            Channel_Data.append(channel_data['Channel_Details'][i])
        
    st_Channel_Data = st.dataframe(Channel_Data,width=2000)
    
    return st_Channel_Data

#To show Video detail in streamlit
def show_video_detail():
    Video_Details=[]
    collection=db["Channel_Data"]
    for video_details in collection.find({},{"_id":0,"Video_Details":1}):
        for i in range(len(video_details['Video_Details'])):
            Video_Details.append(video_details['Video_Details'][i])
        
    st_Video_Details = st.dataframe(Video_Details,width=2000)
    
    return st_Video_Details

#To show Comment detail in streamlit
def show_comment_detail():
    Comment_Details=[]
    collection=db["Channel_Data"]
    for Comment_details in collection.find({},{"_id":0,"Comment_details":1}):
        for i in range(len(Comment_details['Comment_details'])):
            Comment_Details.append(Comment_details['Comment_details'][i])
    
    st_Comment_details= st.dataframe(Comment_Details,width=2000)
    
    return st_Comment_details

#To show Playlist detail in streamlit
def show_playlist_detail():
    Playlist_Details=[]
    collection=db["Channel_Data"]
    for Playlist_details in collection.find({},{"_id":0,"Playlist_Details":1}):
        for i in range(len(Playlist_details['Playlist_Details'])):
            Playlist_Details.append(Playlist_details['Playlist_Details'][i])
        
    st_Playlist_Details = st.dataframe(Playlist_Details,width=2000)
    
    return st_Playlist_Details

#StreamLit 
client=pymongo.MongoClient("mongodb+srv://akshaya08:Abhi1%40aks@cluster0.s7e4jmk.mongodb.net/")
db=client["Youtube_data"]
cursor,mydb = sql_connect()

st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

#tabs in Streamlit
selection = option_menu(
    menu_title= None,
    options=["Fetch Data","Migrate Data","Show Data","Data Analysis"],
    icons = ["youtube","database-fill-add","database",'bar-chart-line-fill'],
    default_index=0,
    orientation="horizontal")


if selection == "Fetch Data":
    st.subheader("Enter the YouTube Channel Id")
    channel_id=st.text_input("")   
    if st.button("Fetch and Save"):
        ch_ids=[]
        collection=db["Channel_Data"]
        for channel_data in collection.find({},{"_id":0,"Channel_Details":1}):
            ch_ids.append(channel_data["Channel_Details"][0]['Channel_Id'])
            
        if channel_id not in ch_ids:
            insert=channel_details(channel_id)
            st.success(insert)
            
        else:
            st.error("Channel Details Already Exists")

if selection == "Migrate Data":
    all_channels=[]
    collection=db["Channel_Data"]
    for channel_data in collection.find({},{"_id":0,"Channel_Details":1}):
        all_channels.append(channel_data["Channel_Details"][0]["Channel_name"])
        
    unique_channel= st.selectbox("Select the Channel",all_channels)

    if st.button("Migrate to Sql"):
        Table=tables(unique_channel)
        if Table == True:
            st.success("SQL Migration Successful")
        else:
            st.error("Data Already Exists")
           
if selection == "Show Data":
    st.subheader("Select the Table for View")
    show_table=st.radio("",("CHANNELS","PLAYLIST","VIDEOS","COMMENTS"))
            
    if show_table=="CHANNELS":
        show_channel_detail()
        
    elif show_table=="VIDEOS":
        show_video_detail()
        
    elif show_table=="COMMENTS":
        show_comment_detail()

    elif show_table=="PLAYLIST":
        show_playlist_detail()
        
if selection == "Data Analysis":   
    Query = st.selectbox("Select your Query",("Select Query from the drop down",
                                                    "1. Show All the Videos and Channel name",
                                                    "2. Channels with most number of Videos and Show Total Video Count",
                                                    "3. 10 Most Viewed Videos with Channel name",
                                                    "4. Comment count with Video Title",
                                                    "5. Highest number of likes with Channel name",
                                                    "6. Total number of likes count for each video with Video Title",
                                                    "7. Total number of views for each channel with Channel name",
                                                    "8. Names of all the channels that have published videos in the year 2022",
                                                    "9. Average Duration of all videos in each channel with Channel Name",
                                                    "10.Videos with Highest Comment with Channel Name"))

    if Query == "1. Show All the Videos and Channel name":
        query1 =''' SELECT Video_Title AS Video_Title, 
                    Channel_name AS Channel_Name from video_data'''
        cursor.execute(query1)
        mydb.commit()
        table1 = cursor.fetchall()
        df1 = pd.DataFrame(table1, columns=["Video Title","Channel Name"])
        df1.index = df1.index + 1
        st.dataframe(df1,width=2000)
        
    elif Query == "2. Channels with most number of Videos and Show Total Video Count":
        query2 = ''' SELECT Channel_name AS Channel_Name, Channel_Total_Videos AS Total_Videos from Channel_data
                    ORDER BY Channel_Total_Videos'''
        cursor.execute(query2)
        mydb.commit()
        table2 = cursor.fetchall()
        df2 = pd.DataFrame(table2, columns=["Channel Name","Total Number of Videos"])
        df2.index = df2.index + 1
        st.dataframe(df2,width=1000)
        on = st.toggle('Show Chart')
        if on:
            sns.barplot(x='Channel Name', y='Total Number of Videos',data=df2,palette='Set2')
            plt.xlabel('Channel Name',fontsize=16,color='r')
            plt.ylabel('Total Number of Videos',fontsize=16,color='r')
            plt.xticks(rotation = 45)
            st.set_option('deprecation.showPyplotGlobalUse', False)
            st.pyplot()
        
        
    elif Query == "3. 10 Most Viewed Videos with Channel name":
        query3 = ''' SELECT Video_Title AS Video_Title,
                    Video_Views AS Video_Views, 
                    Channel_name AS Channel_Name from video_data
                    WHERE Video_Views IS NOT NULL ORDER BY Video_Views DESC LIMIT 10'''
        cursor.execute(query3)
        mydb.commit()
        table3 = cursor.fetchall()
        df3 = pd.DataFrame(table3, columns=["Video Title","Video Views","Channel_Name"])
        df3.index = df3.index + 1
        st.dataframe(df3,width=2000)
        
    elif Query == "4. Comment count with Video Title":
        query4 = ''' SELECT Video_Title AS Video_Title,
                    Video_Comments AS Video_Comments, 
                    Channel_name AS Channel_Name from video_data 
                    WHERE Video_Comments IS NOT NULL
                    ORDER BY Video_Comments DESC'''
        cursor.execute(query4)
        mydb.commit()
        table4 = cursor.fetchall()
        df4 = pd.DataFrame(table4, columns=["Video Title","Total Video Comments","Channel_Name"])
        df4.index = df4.index + 1
        st.dataframe(df4,width=2000)

    elif Query == "5. Highest number of likes with Channel name":
        query5 = '''SELECT Channel_name AS Channel_Name,
                    Video_Title AS Video_Title,
                    Video_Likes AS Video_Likes
                FROM (
                    SELECT Channel_name, Video_Title, Video_Likes,
                            ROW_NUMBER() OVER (PARTITION BY Channel_name ORDER BY Video_Likes DESC) AS RowNum
                    FROM video_data
                    WHERE Video_Likes IS NOT NULL
                ) AS RankedVideos
                WHERE RowNum = 1
                ORDER BY Video_Likes'''
        cursor.execute(query5)
        mydb.commit()
        table5 = cursor.fetchall()
        df5 = pd.DataFrame(table5, columns=["Channel_Name","Video Title","Highest Video Likes"])
        df5.index = df5.index + 1
        st.dataframe(df5,width=1000)
        on = st.toggle('Show Chart')
        if on:
            sns.barplot(x='Channel_Name', y='Highest Video Likes',data=df5,palette='Set2')
            plt.xlabel('Channel_Name',fontsize=16,color='r')
            plt.ylabel('Highest Video Likes',fontsize=16,color='r')
            plt.xticks(rotation = 45)
            sns.set_style("whitegrid")
            st.set_option('deprecation.showPyplotGlobalUse', False)
            st.pyplot()
        
    elif Query == "6. Total number of likes count for each video with Video Title":
        query6 = ''' SELECT Video_Title AS Video_Title,
                    Video_Likes AS Video_Likes from video_data 
                    WHERE Video_Likes IS NOT NULL
                    ORDER BY Video_Likes DESC'''
        cursor.execute(query6)
        mydb.commit()
        table6 = cursor.fetchall()
        df6 = pd.DataFrame(table6, columns=["Video Title","Total Video Likes"])
        df6.index = df6.index + 1
        st.dataframe(df6,width=2000)
        
    elif Query == "7. Total number of views for each channel with Channel name":
        query7 = ''' SELECT Channel_name AS Channel_Name, 
                    Channel_Views AS Channel_Views from Channel_data
                    ORDER BY Channel_Views '''
        cursor.execute(query7)
        mydb.commit()
        table7 = cursor.fetchall()
        df7 = pd.DataFrame(table7, columns=["Channel Name","Total Number of Views"])
        df7.index = df7.index + 1
        st.dataframe(df7,width=1000)
        on = st.toggle('Show Chart')
        if on:
            sns.barplot(x='Channel Name', y='Total Number of Views',data=df7,palette='Set2')
            plt.xlabel('Channel Name',fontsize=16,color='r')
            plt.ylabel('Total Number of Views',fontsize=16,color='r')
            plt.xticks(rotation = 45)
            sns.set_style("whitegrid")
            st.set_option('deprecation.showPyplotGlobalUse', False)
            st.pyplot()
          
    elif Query == "8. Names of all the channels that have published videos in the year 2022":
        query8 = ''' SELECT Channel_name AS Channel_Name,
                    Video_Title AS Video_Title,
                    Video_Publisheddate AS Video_Publisheddate from video_data 
                    WHERE EXTRACT(YEAR FROM Video_Publisheddate)=2022'''
        cursor.execute(query8)
        mydb.commit()
        table8 = cursor.fetchall()
        df8 = pd.DataFrame(table8, columns=["Channel Name","video Title","Year"])
        df8.index = df8.index + 1
        st.dataframe(df8,width=2000)

    elif Query == "9. Average Duration of all videos in each channel with Channel Name":
        query9 = ''' SELECT Channel_name AS Channel_Name,
                    AVG(Video_Duration) AS AVG_Video_Duration from video_data 
                    GROUP BY Channel_name '''
        cursor.execute(query9)
        mydb.commit()
        table9 = cursor.fetchall()
        df9 = pd.DataFrame(table9, columns=["Channel Name","Average Video Duration"])
        def seconds_to_time_string(seconds):
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            seconds = int(seconds % 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # Apply the function to create a new column with formatted duration
        df9['Average Video Duration'] = df9['Average Video Duration'].apply(seconds_to_time_string)
        df9.index = df9.index + 1
        st.dataframe(df9,width=1000)
    
    elif Query == "10.Videos with Highest Comment with Channel Name":
        query10 = ''' SELECT Channel_name AS Channel_Name,
                    Video_Title AS Video_Title,
                    Video_Comments AS Total_Comments 
                    FROM (
                    SELECT Channel_name, Video_Title, Video_Comments,
                            ROW_NUMBER() OVER (PARTITION BY Channel_name ORDER BY Video_Comments DESC) AS RowNum
                    FROM video_data
                    WHERE Video_Comments IS NOT NULL
                    ) AS RankedVideos
                    WHERE RowNum = 1
                    ORDER BY Video_Comments '''
        cursor.execute(query10)
        mydb.commit()
        table10 = cursor.fetchall()
        df10 = pd.DataFrame(table10, columns=["Channel Name","Video Title","Total Number of Comments"])
        df10.index = df10.index + 1
        st.dataframe(df10,width=2000)
        on = st.toggle('Show Chart')
        if on:
            sns.barplot(x='Channel Name', y='Total Number of Comments',data=df10,palette='Set2')
            plt.xlabel('Channel Name',fontsize=16,color='r')
            plt.ylabel('Total Number of Comments',fontsize=16,color='r')
            plt.xticks(rotation = 45)
            sns.set_style("whitegrid")
            st.set_option('deprecation.showPyplotGlobalUse', False)
            st.pyplot()


