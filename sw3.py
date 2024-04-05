import pymongo
import googleapiclient.discovery
import mysql.connector
import pandas as pd
from dateutil import parser
import streamlit as st
import certifi
ca = certifi.where()
conn_obj = pymongo.MongoClient("mongodb+srv://anbarasi1092:Anba1992@cluster0.7d4ovmt.mongodb.net/?retryWrites=true&w=majority&appName=cluster0",tlsCAFile=ca)
youtubedh_db = conn_obj["Youtube"]
yt_collection = youtubedh_db['channel_info']
custom_css = """
<style>
body {
    background-color:  #98FB98; /* Set your desired background color */
}
</style>
"""

# Inject custom CSS
st.markdown(custom_css, unsafe_allow_html=True)
st.title(':red[YouTube Harvesting Data]')
st.header(':green[Data collection]')
st.write('(Note:-This zone **collect data** by using channel id and stored it in the MonogDB database)')

api_key = "AIzaSyDnFu7dnDFoVPYUJek8G9MrcX-tRCkiVnM"
api_service_name = "youtube"
api_version = "v3"

# Create a YouTube API client
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,statistics,status,contentDetails",
        id=channel_id
    )
    ch_response = request.execute()

    channels = []
    for cha in ch_response['items']:
        channel_id = cha['id']
        channel_name = cha['snippet']['title']
        channel_descri = cha['snippet']['description']
        channel_published_at = cha['snippet']['publishedAt']
        playlists = cha.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', 'Unknown')
        statistics = cha.get('statistics', {})
        channel_like = statistics.get('likeCount', 'Unknown')
        channel_viewcount = statistics.get('viewCount', 'Unknown')
        channel_sub_count = statistics.get('subscriberCount', 'Unknown')
        channel_status = cha['status'].get('privacyStatus', 'Unknown')
        channel_video_count = statistics.get('videoCount', 'Unknown')

        channels.append({"channel_id": channel_id,
                         "channel_name": channel_name,
                         "channel_descri": channel_descri,
                         "playlists":  playlists,
                         "channel_status": channel_status,
                         "channel_published_at": channel_published_at,
                         "channel_like": channel_like,
                         "channel_viewcount": channel_viewcount,
                         "channel_sub_count": channel_sub_count,
                         "channel_video_count": channel_video_count,
                         })
    return channels


def fetch_comments(video_id):
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
        )
        response = request.execute()
        comments = []
        count = 0
        for item in response.get('items', []):
            snippet = item['snippet']['topLevelComment']['snippet']
            comment_id = item['id']
            comment_video_id = snippet.get('videoId')
            comment_textdisplay = snippet.get('textDisplay', 'Unknown')
            comment_authorname = snippet['authorDisplayName']
            comment_authorchannelid = snippet['authorChannelId']['value']
            comment_channelID = snippet['channelId']
            comment_viewerrating = snippet.get('viewerRating', 'Unknown')
            comment_likecount = snippet['likeCount']
            comment_updateAt = snippet['updatedAt']

            comments.append({
                "comment_id": comment_id,
                "comment_video_id": comment_video_id,
                "comment_textdisplay": comment_textdisplay,
                "comment_authorname": comment_authorname,
                "comment_authorchannelid": comment_authorchannelid,
                "comment_channelID": comment_channelID,
                "comment_viewerrating": comment_viewerrating,
                "comment_likecount": comment_likecount,
                "comment_updateAt": comment_updateAt
            })
            count += 1
            if count <= 150:
                break
        return comments
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 403:
            print(f"Comments are disabled for video with ID {video_id}. Skipping...")
            return []
        else:
            raise
def get_videos_details(channel_id):
    videos = []
    next_page_token = None

    while True:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            type="video",
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        video_ids = [item['id']['videoId'] for item in response['items']]

        # Debug output to check video IDs in response items
        for item in response['items']:
            if 'id' in item and 'videoId' in item['id']:
                video_id = item['id']['videoId']
            else:
                print("Video ID not found in item:", item)

        video_request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids)
        )
        video_response = video_request.execute()


        for item in video_response['items']:
            video_id = item['id']
            video_snippet = item['snippet']
            video_statistics = item['statistics']
            video_content_details = item['contentDetails']
            video_published_at = video_snippet['publishedAt']
            video_name = video_snippet['title']
            video_description = video_snippet['description']
            video_channel_id = video_snippet['channelId']
            video_thumbnails = video_snippet['thumbnails']['default']['url']
            video_channel_name = video_snippet['channelTitle']
            video_like_count = video_statistics.get('likeCount', 'Unknown')
            video_dislike_count = video_statistics.get('dislikeCount', 'Unknown')
            video_favorite = video_statistics.get('favoriteCount', 'Unknown')
            view_count = video_statistics.get('viewCount', 'Unknown')
            video_duration = video_content_details['duration']
            caption_status = video_content_details['caption']
            comment_count = video_statistics.get('commentCount', 'Unknown')

            try:
                comments = fetch_comments(video_id)
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 403:
                    print(f"Comments are disabled for video with ID {video_id}. Skipping...")
                    comments = []
            #  Calculate video duration in secondssw2.py

            video_duration_seconds = duration_to_seconds(video_duration)

            videos.append({
                "video_id": video_id,
                "video_name": video_name,
                "video_description": video_description,
                "video_published": video_published_at,
                "video_channelID": video_channel_id,
                "video_thumbnail": video_thumbnails,
                "video_channel_name": video_channel_name,
                "video_favorite": video_favorite,
                "view_count": view_count,
                "video_like_count": video_like_count,
                "video_dislike_count": video_dislike_count,
                "video_duration": video_duration,
                "video_duration_seconds": video_duration_seconds,  # Add video_duration_seconds here
                "caption_status": caption_status,
                "comment_count": comment_count,

            })

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return videos

def duration_to_seconds(duration_str):
# Remove the 'PT' prefix and 'S' suffix
    duration_str = duration_str[2:-1]

    # Initialize variables to store hours, minutes, and seconds
    hours = minutes = seconds = 0

    # Split the duration string into parts based on 'H', 'M', and 'S'
    parts = duration_str.split('H')
    if len(parts) == 2:  # If duration contains hours and minutes or only hours
        hours = int(parts[0])
        if 'M' in parts[1]:
            minutes_parts = parts[1].split('M')
            minutes = int(minutes_parts[0])
            if 'S' in minutes_parts[1]:
                seconds = int(minutes_parts[1].split('S')[0])
        elif 'S' in parts[1]:
            seconds = int(parts[1].split('S')[0])
    elif len(parts) == 1:  # If duration contains only minutes and seconds or only seconds
        if 'M' in parts[0]:
            minutes_parts = parts[0].split('M')
            minutes = int(minutes_parts[0])
            if 'S' in minutes_parts[1]:
                seconds = int(minutes_parts[1].split('S')[0])
        elif 'S' in parts[0]:
            seconds = int(parts[0].split('S')[0])

    # Convert hours, minutes, and seconds to total seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

conn_obj = pymongo.MongoClient("mongodb+srv://anbarasi1092:Anba1992@cluster0.5g34nei.mongodb.net/")
youtubedh_db = conn_obj["Youtube"]
yt_collection = youtubedh_db["channel_info"]
mydb = mysql.connector.connect(
                                host='localhost',
                                user='root',
                                password='root',
                                database='youtube_details',
                                auth_plugin='mysql_native_password',
                                charset='utf8mb4'
                                  )
mycursor = mydb.cursor()
mydb.commit()

def create_channel_table():
    mycursor.execute("""CREATE TABLE IF NOT EXISTS Channels (channel_id VARCHAR(255) PRIMARY KEY,
                                                channel_name VARCHAR(255),
                                                channel_description TEXT,
                                                playlists VARCHAR(255),
                                                channel_status VARCHAR(255),
                                                channel_published_at DATETIME,
                                                channel_like VARCHAR(255),
                                                channel_viewcount INT,
                                                channel_sub_count INT,
                                                channel_video_count INT)""")

    mydb.commit()
def insert_channel_table(selected_channel):
   row = yt_collection.find_one({'channel_information.0.channel_name':selected_channel})['channel_information']
   sql = """INSERT INTO Channels 
             (channel_id, channel_name, channel_description, playlists, channel_status, channel_published_at, 
              channel_like, channel_viewcount, channel_sub_count, channel_video_count) 
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON DUPLICATE KEY UPDATE channel_id = VALUES(channel_id)

            """
   values = (row[0]['channel_id'],
              row[0]["channel_name"],
              row[0]["channel_descri"],
              row[0]["playlists"],
              row[0]["channel_status"],
              parser.parse(row[0]["channel_published_at"]),
              row[0]["channel_like"],
              row[0]["channel_viewcount"],
              row[0]["channel_sub_count"],
              row[0]["channel_video_count"])

   mycursor.execute(sql, values)
   mydb.commit()

def create_video_table():
    mycursor.execute("""CREATE TABLE IF NOT EXISTS videos (
                        video_id VARCHAR(255) PRIMARY KEY,
                        video_name VARCHAR(255),
                        video_description TEXT,
                        video_published DATETIME,
                        video_channelID VARCHAR(255),
                        video_channel_name VARCHAR(255),
                        video_thumbnail VARCHAR(255),
                        video_favorite VARCHAR(255),
                        view_count INT,
                        video_like_count VARCHAR(255),
                        video_dislike_count VARCHAR(255),
                        video_duration VARCHAR(255),
                        video_duration_seconds INT,
                        caption_status VARCHAR(255),
                        comment_count INT)
                """)

    mydb.commit()
def insert_video_table(selected_channel):
    vd = yt_collection.find_one({'channel_information.0.channel_name':selected_channel})['videos_information']
    for i in range(len(vd)):
        row = vd[i]
        video_published= parser.parse(row['video_published'])
         # Handle missing or unknown comment_count
        comment_count = row.get("comment_count", None)
        if comment_count is not None and comment_count.isdigit():
            comment_count = int(comment_count)
        else:
            comment_count = None
        sql  = """
                    INSERT INTO videos 
                    (video_id, video_name, video_description, video_published, video_channelID, video_thumbnail, 
                    video_favorite, view_count, video_like_count, video_dislike_count, video_duration, 
                    video_duration_seconds, caption_status, comment_count) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE video_id = VALUES(video_id)"""
        values  =        ( row["video_id"],
                          row["video_name"],
                          row["video_description"],
                          video_published,
                          row["video_channelID"],
                          row["video_thumbnail"],
                          row["video_favorite"],
                          row["view_count"],
                          row["video_like_count"],
                          row["video_dislike_count"],
                          row["video_duration"],
                          row["video_duration_seconds"],
                          row["caption_status"],
                          row["comment_count"]
                              )

        mycursor.execute(sql, values)
        mydb.commit()



def create_comment_table():
    mycursor.execute("""  CREATE TABLE IF NOT EXISTS comments (
                                                comment_id VARCHAR(255) PRIMARY KEY,
                                                comment_video_id VARCHAR(255),
                                                comment_textdisplay TEXT,
                                                comment_authorname VARCHAR(255),
                                                comment_authorchannelid VARCHAR(255),
                                                comment_channelid VARCHAR(255),
                                                comment_viewerrating VARCHAR(255),
                                                comment_likecount INT,
                                                comment_updateAt DATETIME)
                                            """ )
    mydb.commit()
def insert_comment_table(selected_channel):

    com = yt_collection.find_one({'channel_information.0.channel_name':selected_channel})['comment_information']
    for i in range(len(com)):
        row = com[i]
        comment_updateAt  = parser.parse(row["comment_updateAt"])
        sql = """
                INSERT INTO comments
                (comment_id, comment_video_id, comment_textdisplay, comment_authorname, comment_authorchannelid, comment_channelID,
                comment_viewerrating, comment_likecount, comment_updateAt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                comment_id = VALUES(comment_id),
                comment_textdisplay= VALUES(comment_textdisplay)
                """
        values = (
                    row["comment_id"],
                    row["comment_video_id"],
                    row["comment_textdisplay"],
                    row["comment_authorname"],
                    row["comment_authorchannelid"],
                    row["comment_channelID"],
                    row["comment_viewerrating"],
                    row["comment_likecount"],
                    comment_updateAt
                   )

        mycursor.execute(sql, values)
        mydb.commit()
    return "comment table inserted data"

conn_obj = pymongo.MongoClient("mongodb+srv://anbarasi1092:Anba1992@cluster0.7d4ovmt.mongodb.net/?retryWrites=true&w=majority&appName=cluster0",tlsCAFile=ca)

def youchannel_details(c_id):
    ch_info = get_channel_details(c_id)
    if ch_info:
        video_info = get_videos_details(c_id)
        if video_info:
            comm_info = []
            dur_info = []

            for vid in video_info:
                video_id = vid['video_id']
                comments = fetch_comments(video_id)
                if comments:
                    comm_info.extend(comments)

                    # Calculate duration for each video and insert into video information
                    durat_str = vid['video_duration']
                    duration_seconds = duration_to_seconds(durat_str)
                    vid['video_duration_seconds'] = duration_seconds  # Modify 'vid' dictionary instead of 'video'
                    dur_info.append(duration_seconds)

            # Data insertion into MongoDB collection
            A = {
                "channel_information": ch_info,
                "videos_information": video_info,
                "comment_information": comm_info,
                "duration_information": dur_info
            }
            yt_collection.insert_one(A)
            st.success('Data collected and stored successfully!')
        else:
            st.error("Failed to get video information.")
    else:
        st.error("Failed to get channel information.")

channel_id = st.text_input('Enter the Channel ID')
get_data = st.button('**Get data and stored**')

if get_data and channel_id:  # User entered a new channel ID
    youchannel_details(channel_id)

else:  # User didn't enter a new channel ID, display stored data
    all_cn = [' ']

    chann = yt_collection.find({}, {'_id': 0, 'channel_information.channel_name': 1})
    for doc in chann:
        all_cn.append(doc['channel_information'][0]['channel_name'])
cn = all_cn

selected_channel = st.selectbox("Pick a Channel", options=cn)
if selected_channel != ' ':
    st.write(selected_channel)
    channel_data = yt_collection.find_one({'channel_information.0.channel_name':selected_channel},{'_id':0,'channel_information':1})
    st.write(channel_data)
    video_data = yt_collection.find_one({'channel_information.0.channel_name':selected_channel},{'_id':0,'videos_information':1})
    st.write(video_data)
    comment_data = yt_collection.find_one({'channel_information.0.channel_name':selected_channel},{'_id':0,'comment_information':1})
    st.write(comment_data)

st.header(":green[Data Migration]")
if "migration_button_clicked" not in st.session_state:
    st.session_state["migration_button_clicked"] = False

if st.button("Migarte to MySQL"):
    st.session_state["migration_button_clicked"] = True
selected_table = st.selectbox("Select table to view", options = ['Channels', 'Videos', 'Comments'])
if selected_table == 'Channels':
    try:
        def fetch_channel_data(selected_channel):
            create_channel_table()
            insert_channel_table(selected_channel)
            mycursor.execute("SELECT channel_id, channel_name, channel_status, channel_description, playlists FROM Channels")
            return mycursor.fetchall()
        if selected_channel:
            channel_info = fetch_channel_data(selected_channel)
            df = pd.DataFrame(channel_info, columns=['Channel ID', 'Channel Name', 'Channel Status', 'Channel Description', 'Channel Type'])
            st.table(df)
            st.success("Data migrated successfully!")
    except:
        pass
elif selected_table == 'Videos':
    def fetch_video_data(selected_channel):
        create_video_table()
        insert_video_table(selected_channel)

        mycursor.execute("SELECT  video_id, video_channelID, video_description FROM Videos")
        return mycursor.fetchall()
    if selected_channel:
        video_data = fetch_video_data(selected_channel)
        df = pd.DataFrame(video_data, columns=['Video_id','Video_ChannelID', 'Video_description'])
        st.table(df)
        st.success('Video data migrated successfully')
elif selected_table == 'Comments':
    def fetch_comment_data(selected_channel):
        create_comment_table()
        insert_comment_table(selected_channel)
        mycursor.execute("SELECT comment_id, comment_channelID, comment_authorname FROM Comments")
        return mycursor.fetchall()
    if selected_channel:
        comment_data = fetch_comment_data(selected_channel)
        df = pd.DataFrame(comment_data, columns=['comment_id','comment_channelID','comment_atuthorname'])
        st.table(df)
        mydb.close()


mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='youtube_details',
    charset='utf8mb4'
)
mycursor= mydb.cursor()
mydb.commit()

if "selectbox_enabled" not in st.session_state:
    st.session_state["selectbox_enabled"] = False

def execute_query(selected_option: str):

    if selected_option == "1. What are the names of all the videos and their corresponding channels?":
        mycursor.execute("""SELECT channels.channel_name, videos.video_name 
                            FROM videos
                            JOIN channels ON videos.video_channelID = channels.channel_id 
                            ORDER BY channels.channel_name

                            """)
        result1 = mycursor.fetchall()
        df1 = pd.DataFrame(result1, columns=["Channel_name", "Video Name"]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    elif selected_option == "2. Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("""SELECT channel_name, channel_video_count FROM channels WHERE channel_video_count IN (SELECT MAX(channel_video_count) FROM channels)""")
        result2 = mycursor.fetchall()
        df2 = pd.DataFrame(result2, columns=["Channel_Name", "Total number of Videos"]).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)

    elif selected_option == "3. What are the top 10 most viewed videos and their respective channels?":
            mycursor.execute("""SELECT channels.channel_name, videos.video_name, videos.view_count 
                                FROM videos 
                                LEFT JOIN channels ON videos.video_channelID = channels.channel_id 
                                ORDER BY videos.view_count DESC 
                                LIMIT 10
                                """)
            result3 = mycursor.fetchall()
            df3 = pd.DataFrame(result3, columns=["channel_name", "Video Name", "Views"]).reset_index(drop=True)
            df3.index += 1
            st.dataframe(df3)

    elif selected_option == "4. How many comments were made on each video, and what are their corresponding channel names?":
            mycursor.execute("""SELECT video_name, COUNT(comment_id) AS num_comments FROM videos LEFT JOIN comments 
                            ON videos.video_id = comments.comment_video_id GROUP BY video_name;
                            """)
            result4 = mycursor.fetchall()
            df4 = pd.DataFrame(result4, columns=["Video Name", "Comment Count"]).reset_index(drop=True)
            df4.index += 1
            st.dataframe(df4)
#"5. Which videos have the highest number of likes, and what are their corresponding channel names?"
    elif selected_option == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            mycursor.execute("""SELECT videos.video_like_count, videos.video_name, channels.channel_name
                                FROM videos
                                JOIN channels ON videos.video_channelID = channels.channel_id
                                ORDER BY videos.video_like_count DESC
                                LIMIT 10""")
            result5 = mycursor.fetchall()
            print("Result of SQL query:")
            print(result5)  # Or whichever result variable corresponds to your query
            df5 = pd.DataFrame(result5, columns=["Video like", "Video Name", "Channel Name"]).reset_index(drop=True)
            print("DataFrame df5:")
            print(df5)
            df5.index += 1

            st.dataframe(df5)

    elif selected_option == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("""SELECT video_name, 
                           SUM(video_like_count) AS total_likes, 
                           SUM(video_dislike_count) AS total_dislikes 
                           FROM videos 
                           GROUP BY video_name""")
        result6 = mycursor.fetchall()
        df6 = pd.DataFrame(result6, columns=["video Name", "total_likes", "total_dislikes"]).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)
#"7. "7. What is the total number of views for each channel, and what are their corresponding channel names?"
    elif selected_option == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("""SELECT c.channel_name, SUM(v.view_count) AS total_views
                            FROM channels AS c
                            LEFT JOIN videos AS v ON c.channel_id = v.video_channelID
                            GROUP BY c.channel_name
                                    """)
        result7 = mycursor.fetchall()
        df7 = pd.DataFrame(result7, columns=["channel_name", "Total number of views"]).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)

    elif selected_option == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("""SELECT DISTINCT c.channel_name, v.video_published AS published_date
                            FROM channels c INNER JOIN videos v ON c.channel_id = v.video_channelID
                            WHERE YEAR(v.video_published) = 2022
                             """)
        result8 = mycursor.fetchall()
        df8 = pd.DataFrame(result8, columns=["channel_name", "Published Date"]).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    elif selected_option == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            mycursor.execute("""SELECT c.channel_name, AVG(v.video_duration_seconds) AS average_duration_seconds
                                FROM videos AS v
                                JOIN channels AS c ON v.video_channelID = c.channel_id
                                GROUP BY c.channel_name """)
            result9 = mycursor.fetchall()
            df9 = pd.DataFrame(result9, columns=["channel_name", "average_duration_seconds"]).reset_index(drop=True)
            df9.index += 1
            st.dataframe(df9)

    elif selected_option == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            mycursor.execute("""SELECT videos.comment_count,videos.video_name, channels.channel_name from videos 
            join channels on 
            videos.video_channelID = channels.channel_id order by comment_count desc limit 10""")

            result10 = mycursor.fetchall()
            df10 = pd.DataFrame(result10, columns=["channel_name", "Video Name", "Comment Count"]).reset_index(drop=True)
            df10.index += 1
            st.dataframe(df10)

selected_option = st.selectbox(
    "Choose the question you want to answer using an SQL query",
    ["1. What are the names of all the videos and their corresponding channels?",
     "2. Which channels have the most number of videos, and how many videos do they have?",
     "3. What are the top 10 most viewed videos and their respective channels?",
     "4. How many comments were made on each video, and what are their corresponding channel names?",
     "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
     "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
     "7. What is the total number of views for each channel, and what are their corresponding channel names?",
     "8. What are the names of all the channels that have published videos in the year 2022?",
     "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
     "10. Which videos have the highest number of comments, and what are their corresponding channel names?"],
    index=None,
    placeholder="Select your Question...")

st.write("Question: ", selected_option)

if selected_option:
    st.session_state["selectbox_enabled"] = True
    execute_query(selected_option)

