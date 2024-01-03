#REQUIRED LIBRARIES
import streamlit as st
import pandas as pd
import numpy as np
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import psycopg2
import pymongo


#SETTING STREAMLIT PAGE
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Jameel",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Jameel!*"""})
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Migrate","View Details","Queries","Drop details from SQL"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
    
#GETTING CONNECTION FOR YOUTUBE API
api_key='Enter your API Key'
youtube=build('youtube','v3',developerKey=api_key)

#FUNCTION FOR GETTING CHANNEL DETAILS
def get_channel_stats(channel_id):
  request=youtube.channels().list(
      part='snippet,contentDetails,statistics',
      id=channel_id)
  response=request.execute()
  for i in range(len(response['items'])):
    data=dict(Channel_Name=response['items'][i]['snippet']['title'],
              Channel_Id=response['items'][i]['id'],
              Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
              Channel_Views=response['items'][i]['statistics']['viewCount'],
              Channel_Description=response['items'][i]['snippet']['description'],
              Total_videos = response['items'][i]['statistics']['videoCount'],
              Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
  return data

#FUNCTION FOR GETTING PLAYLIST DETAILS
def get_playlist_details(channel_id):
  playlist_data=[]
  next_page_token=None
  next_page=True
  while next_page:
    request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
    )
    response=request.execute()
    for item in response['items']: 
              data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
              playlist_data.append(data)
    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
      next_page=False
  return playlist_data

#FUNCTION FOR GETTING VIDEO IDS
def get_video_ids(channel_id):
  Playlist_Ids=[]
  response=youtube.channels().list(
                                id=channel_id,
                                part='contentDetails').execute()
  for i in range(len(response['items'])):
    Playlist_Ids.append(response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
  video_ids=[]
  for pl in Playlist_Ids:
    next_page_token=None
    while True:
      response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=pl,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
      for i in range(len(response1['items'])):
        video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token=response1.get('nextPageToken')
      if next_page_token is None:
        break
  return video_ids

#FUNCTION FOR GETTING COMMENT DETAILS
def get_comment_details(video_ids):
  Comments=[]
  try:
    for video_id in video_ids:
      request=youtube.commentThreads().list(
                                            part='snippet,replies',
                                            videoId=video_id,
                                            maxResults=100
      )
      response=request.execute()
      for item in response['items']:
          data = dict(Comment_Id = item['id'],
                      Channel_Id = item['snippet']['channelId'],
                      Video_Id = item['snippet']['videoId'],
                      Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                      Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                      Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt'],
                      Reply_count = item['snippet']['totalReplyCount']
                      )
          Comments.append(data)
  except:
    pass
  return Comments

#FUNCTION FOR DURATION TO CONVERT INDIAN TIME
def time_duration(t):
        a = pd.Timedelta(t)
        b = str(a).split()[-1]
        return b

#FUNCTION FOR GETTING VIDEO DETAILS
def get_video_details(video_ids):
  try:
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()
        for video in response['items']:
            data=dict(Channel_Name = video['snippet']['channelTitle'],
                      Channel_Id = video['snippet']['channelId'],
                      Video_Id=video['id'],
                      Video_Name=video['snippet']['title'],
                      Video_Description=video['snippet'].get('description'),
                      Tags=video['snippet'].get('tags'),
                      PublishedAt=video['snippet']['publishedAt'],
                      View_Count=video['statistics'].get('viewCount'),
                      Like_Count=video['statistics'].get('likeCount'),
                      Favorite_Count=video['statistics']['favoriteCount'],
                      Comment_Count=video['statistics'].get('commentCount'),
                      Duration=time_duration(video['contentDetails']['duration']),
                      Thumbnail=video['snippet']['thumbnails']['default']['url'],
                      Caption_Status=video['contentDetails']['definition']
                      )
            video_data.append(data)
  except:
    pass
  return video_data

#CONNECTING MDB
client=pymongo.MongoClient('Enter your drivers link')
db=client['Youtube_data']

#FUNCTION FOR INSERTING DATA TO MDB
def insert_to_mdb(channel_id):
  try:
    ch_details=get_channel_stats(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_details(vi_ids)
    com_details=get_comment_details(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                      'video_information':vi_details, 'comment_information':com_details})
    return 'upload completed successfully'
  except:
    pass

#CREATING CHANNEL TABLE
def channel_table():
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    try:
        create_query="""create table channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscription_Count bigint,
                                                        Channel_Views bigint,
                                                        Channel_Description text,
                                                        Total_videos int,
                                                        Playlist_Id varchar(80))"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        pass

#INSERTING CHANNEL VALUES
def insert_channel_details(chn_name):
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({"channel_information.Channel_Name" :chn_name},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)
    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Channel_Views,
                                            Channel_Description,
                                            Total_videos,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Total_videos'],
                row['Playlist_Id'])
    try:
        cursor.execute(insert_query, values)
        mydb.commit()
    except:
        pass

#CREATING PLAYLIST TABLE
def playlist_table():
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    try:
        create_query="""create table playlists(PlaylistId varchar(100) primary key,
                                                Title varchar(100),
                                                ChannelId varchar(100),
                                                ChannelName varchar(100),
                                                PublishedAt timestamp,
                                                VideoCount int)"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        pass

#INSERTING PLAYLIST DETAILS
def insert_playlist_details(chn_name):
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()
    
    pl_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({"channel_information.Channel_Name" :chn_name},{'_id':0,'playlist_information':1}):
        for i in range (len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)
    
    for index,row in df1.iterrows():
        insert_query='''insert into playlists(PlaylistId,
                                                Title,
                                                ChannelId,
                                                ChannelName,
                                                PublishedAt,
                                                VideoCount)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        values=(row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            pass

#CREATING VIDEO TABLE
def video_table():
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    try:
        create_query="""create table videos(Channel_Name varchar(100),
                                            Channel_Id varchar(100),
                                            Video_Id varchar(30) primary key,
                                            Video_Name varchar(150),
                                            Video_Description text,
                                            Tags text,
                                            PublishedAt timestamp,
                                            View_Count bigint,
                                            Like_Count bigint,
                                            Favorite_Count int,
                                            Comment_Count int,
                                            Duration interval,
                                            Thumbnail varchar(200),
                                            Caption_Status varchar(50))"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        pass

#INSERTING VIDEO DETAILS
def insert_video_details(chn_name):
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()
    
    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({"channel_information.Channel_Name" :chn_name},{'_id':0,'video_information':1}):
        for i in range (len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)
    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Video_Name,
                                                Video_Description,
                                                Tags,
                                                PublishedAt,
                                                View_Count,
                                                Like_Count,
                                                Favorite_Count,
                                                Comment_Count,
                                                Duration,
                                                Thumbnail,
                                                Caption_Status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_Name'],
                    row['Video_Description'],
                    row['Tags'],
                    row['PublishedAt'],
                    row['View_Count'],
                    row['Like_Count'],
                    row['Favorite_Count'],
                    row['Comment_Count'],
                    row['Duration'],
                    row['Thumbnail'],
                    row['Caption_Status'])
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except:
                 pass

#CREATING COMMENT TABLE
def comment_table():
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    try:
        create_query="""create table comments(Comment_Id varchar(100) primary key,
                                                Channel_Id varchar(50),
                                                Video_Id varchar(50),
                                                Comment_Text text,
                                                Comment_Author varchar(150),
                                                Comment_PublishedAt timestamp,
                                                Reply_count int)"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        pass

#INSERTING COMMENT DETAILS
def insert_comment_details(chn_name):
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Enter your password',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()
    
    cm_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for cm_data in coll1.find({"channel_information.Channel_Name" :chn_name},{'_id':0,'comment_information':1}):
        for i in range (len(cm_data['comment_information'])):
            cm_list.append(cm_data['comment_information'][i])
    df3=pd.DataFrame(cm_list)
    
    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                Channel_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_PublishedAt,
                                                Reply_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Comment_Id'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt'],
                    row['Reply_count'])
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except:
                 pass

def tables(chn_name):
    channel_table()
    insert_channel_details(chn_name)
    playlist_table()
    insert_playlist_details(chn_name)
    video_table()
    insert_video_details(chn_name)
    comment_table()
    insert_comment_details(chn_name)

    return "Tables created successfully"

#FOR DISPLAYING CHANNEL TABLE
def show_channels_table():
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    return df

#FOR DISPLAYING PLAYLIST TABLE
def show_playlists_table():
    pl_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range (len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df2=st.dataframe(pl_list)
    return df2

#FOR DISPLAYING VIDEOS TABLE
def show_videos_table():
    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range (len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df3=st.dataframe(vi_list)
    return df3

#FOR DISPLAYING COMMENTS TABLE
def show_comments_table():
    cm_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for cm_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range (len(cm_data['comment_information'])):
            cm_list.append(cm_data['comment_information'][i])
    df4=st.dataframe(cm_list)
    return df4

#FUNCTION FOR DROPPING TABLES WHICH ARE NOT REQUIRED FOR QUERIES
def drop_tables(chn_id):
    try:
        mydb=psycopg2.connect(host='localhost',
                                    user='postgres',
                                    password='Enter your password',
                                    database='youtube_data',
                                    port='5432')
        cursor=mydb.cursor()

        qry='''DELETE FROM channels WHERE channel_id = %s'''
        cursor.execute(qry, (chn_id,))
        mydb.commit()
        qry2='''DELETE FROM playlists WHERE channelId = %s'''
        cursor.execute(qry2, (chn_id,))
        mydb.commit()
        qry3='''DELETE FROM videos WHERE channel_id = %s'''
        cursor.execute(qry3, (chn_id,))
        mydb.commit()
        qry4='''DELETE FROM comments WHERE channel_id = %s'''
        cursor.execute(qry4, (chn_id,))
        mydb.commit()
    except:
        pass

    return 'Channels details deleted Successfully'

#FUNCT FOR FETHING CHANNEL NAMES FROM MDB
def channel_names():   
    chnl_name = []
    for i in db.channel_details.find():
        chnl_name.append(i['channel_information']['Channel_Name'])
    return chnl_name

#FUNCT FOR FETHING CHANNEL ID FROM POSTGRESQL
def channel_sql():
    mydb=psycopg2.connect(host='localhost',
                                    user='postgres',
                                    password='Enter your password',
                                    database='youtube_data',
                                    port='5432')
    cursor=mydb.cursor()

    cursor.execute('''SELECT "channel_id" FROM channels''')
    result = cursor.fetchall()
    mydb.commit()

    chnl_id = [row[0] for row in result]
    return chnl_id

if selected == "Home":
    st.title(":green[Youtube Data Harvesting]")
    st.write("## :blue[Domain] : Social Media")
    st.write("## :blue[Tools used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.write("## :blue[Description] : Retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")

# EXTRACT AND TRANSFORM PAGE
elif selected == "Extract & Migrate":
    tab1,tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 MIGRATE $"])
    
    # EXTRACT and UPLOADING TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID Below :")
        channel_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id")
        channels = channel_id.split(',')
        channels = [ch.strip() for ch in channels if ch]
        if channel_id and st.button("Extract Data"):
            for channel in channels:
                ch_details = get_channel_stats(channel)
                st.write(f'#### Channel details are as follows:')
                st.table(ch_details)
        
        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                for channel in channels:
                    ch_ids=[]
                    db=client['Youtube_data']
                    coll1=db['channel_details']
                    for ch_data in coll1.find({},{"_id":0,'channel_information':1}):
                        ch_ids.append(ch_data['channel_information']['Channel_Id'])

                    if channel in ch_ids:
                        st.success('Channel Details of the given channel id already exists')
                    else:
                        insert=insert_to_mdb(channel)
                        st.success(insert)
    #MIGRATION TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel name to migrate to SQL")

        c_names = channel_names()
        chn_name = st.selectbox("Select channel",options= c_names)

        if st.button("Submit"):
          display = tables(chn_name)
          st.success(display)

#VIEW PAGE
elif selected == "View Details":
    show_table=st.radio('Select the table for view',("Channels","Playlists","Videos","Comments"))
    if show_table=='Channels':
        show_channels_table()
    elif show_table=='Playlists':
        show_playlists_table()
    elif show_table=='Videos':
        show_videos_table()
    elif show_table=='Comments':
        show_comments_table()

#QUERY PAGE
elif selected == "Queries":
    try:
        mydb=psycopg2.connect(host='localhost',
                                    user='postgres',
                                    password='Enter your password',
                                    database='youtube_data',
                                    port='5432')
        cursor=mydb.cursor()

        question=st.selectbox('Select your question',('1. What are the names of all the videos and their corresponding channels?',
                                                    '2. Which channels have the most number of videos, and how many videos do they have?',
                                                    '3. What are the top 10 most viewed videos and their respective channels?',
                                                    '4. How many comments were made on each video, and what are their corresponding video names?',
                                                    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                                    '6. What is the total number of likes for each video, and what are their corresponding video names?',
                                                    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                                    '8. What are the names of all the channels that have published videos in the year 2022?',
                                                    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

        if question=='1. What are the names of all the videos and their corresponding channels?':
            query1='''select Channel_Name as channelname, Video_Name as Videos from videos'''
            cursor.execute(query1)
            mydb.commit()
            Q1=cursor.fetchall()
            dfm=pd.DataFrame(Q1,columns=['Channel Names','Video Names'])
            st.write(dfm)

        elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
            query2='''select Channel_Name as channelname, Total_videos as No_of_Videos from channels order by Total_videos desc'''
            cursor.execute(query2)
            mydb.commit()
            Q2=cursor.fetchall()
            dfm2=pd.DataFrame(Q2,columns=['Channel Names','No of Videos'])
            st.write(dfm2)

        elif question=='3. What are the top 10 most viewed videos and their respective channels?':
            query3='''select Channel_Name as channelname, Video_Name as Video_Title, View_Count as Views from videos where View_Count is not null order by View_Count desc limit 10'''
            cursor.execute(query3)
            mydb.commit()
            Q3=cursor.fetchall()
            dfm3=pd.DataFrame(Q3,columns=['Channel Names','Video Name','Views'])
            st.write(dfm3)

        elif question=='4. How many comments were made on each video, and what are their corresponding video names?':
            query4='''select Video_Name as Video_Title, Comment_Count as No_of_comments from videos where Comment_Count is not null'''
            cursor.execute(query4)
            mydb.commit()
            Q4=cursor.fetchall()
            dfm4=pd.DataFrame(Q4,columns=['Video Name','No of Comments'])
            st.write(dfm4)

        elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            query5='''select Channel_Name as channel_name, Video_Name as Video_Title, Like_Count as No_of_likes from videos where Like_Count is not null order by Like_Count desc'''
            cursor.execute(query5)
            mydb.commit()
            Q5=cursor.fetchall()
            dfm5=pd.DataFrame(Q5,columns=['Channel Name','Video Name','No of Likes'])
            st.write(dfm5)

        elif question=='6. What is the total number of likes for each video, and what are their corresponding video names?':
            query6='''select Video_Name as Video_Title, Like_Count as No_of_likes from videos where Like_Count is not null'''
            cursor.execute(query6)
            mydb.commit()
            Q6=cursor.fetchall()
            dfm6=pd.DataFrame(Q6,columns=['Video Name','No of Likes'])
            st.write(dfm6)

        elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            query7='''select Channel_Name as Channel_Name, Channel_Views as No_of_Views from channels where Channel_Views is not null'''
            cursor.execute(query7)
            mydb.commit()
            Q7=cursor.fetchall()
            dfm7=pd.DataFrame(Q7,columns=['Channel Name','No of Views'])
            st.write(dfm7)

        elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
            query8='''select Channel_Name as Channel_Name, Video_Name as Video_Name, PublishedAt as published_date from videos where extract(year from PublishedAt)=2022'''
            cursor.execute(query8)
            mydb.commit()
            Q8=cursor.fetchall()
            dfm8=pd.DataFrame(Q8,columns=['Channel Name','Video_Name','Published Date'])
            st.write(dfm8)

        elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            query9='''select Channel_Name as Channel_Name, AVG(Duration) as average_duration from videos group by Channel_Name'''
            cursor.execute(query9)
            mydb.commit()
            Q9=cursor.fetchall()
            dfm9=pd.DataFrame(Q9,columns=['Channel Name','Average Duration'])
            Tfm=[]
            for index,row in dfm9.iterrows():
                channel_title=row['Channel Name']
                average_duration=row['Average Duration']
                average_duration_str=str(average_duration)
                Tfm.append(dict(Channel_Name=channel_title, Average_Duration=average_duration_str))
            dfme=pd.DataFrame(Tfm)
            st.write(dfme)

        elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            query10='''select Channel_Name as Channel_Name, Video_Name as video_name, Comment_Count as No_of_comments from videos where Comment_Count is not null order by Comment_Count desc'''
            cursor.execute(query10)
            mydb.commit()
            Q10=cursor.fetchall()
            dfm10=pd.DataFrame(Q10,columns=['Channel Name','Video Name','No of Comments'])
            st.write(dfm10)
    except:
        pass

elif selected == 'Drop details from SQL':
    try:
        st.markdown("#   ")
        st.markdown("### Select a channel ID to delete from SQL")

        c_ids = channel_sql()
        chne_id = st.selectbox("Select channel",options= c_ids)

        if st.button("Submit"):
            Delete = drop_tables(chne_id)
            st.success(Delete)
    except:
        pass
