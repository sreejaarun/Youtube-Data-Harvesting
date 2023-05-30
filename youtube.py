import streamlit as st
from googleapiclient.discovery import build
import pymongo
import mysql.connector

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["youtube_data"]

# Set up MySQL connection
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root@3306',
    database='youtube_data'
)
mysql_cursor = mysql_connection.cursor()

# Set up YouTube API
api_key = "AIzaSyBg-klrlq62JuiQqyRfwui3-dFjIjxcZKU"
youtube = build('youtube', 'v3', developerKey=api_key)

# Streamlit app
def main():
    st.set_page_config(page_title="YouTube Analytics Dashboard")
    st.title('YouTube Analytics Dashboard')
    menu = ["Retrieve Data", "Migrate Data", "Search Data"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Retrieve Data":
        retrieve_data()
    elif choice == "Migrate Data":
        migrate_data()
    elif choice == "Search Data":
        search_data()

# Streamlit app
def main():
    st.set_page_config(page_title="YouTube Analytics Dashboard")
    st.title('YouTube Analytics Dashboard')
    menu = ["Retrieve Data", "Migrate Data", "Search Data"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Retrieve Data":
        retrieve_data()
    elif choice == "Migrate Data":
        migrate_data()
    elif choice == "Search Data":
        search_data()

# Retrieve channel names from MongoDB
def get_channel_names():
    channel_names = mongo_db.channels.distinct("channel_name")
    return channel_names

# Retrieve data from YouTube API and store in MongoDB
def retrieve_data():
    st.header("Retrieve Data")
    channel_id = st.text_input("Enter YouTube Channel ID")
    
    if st.button("Retrieve"):
        channel_data = get_channel_data(channel_id)
        video_data = get_video_data(channel_id)
        
        # Store channel data in MongoDB
        mongo_db.channels.insert_one(channel_data)
        
        # Store video data in MongoDB
        mongo_db.videos.insert_many(video_data)
        
        st.success("Data retrieved and stored successfully.")

# Get channel data using YouTube API
def get_channel_data(channel_id):
    response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()
    
    channel_data = response["items"][0]
    video_count = channel_data["statistics"].get("videoCount", 0)
    channel_data["total_video_count"] = int(video_count)
    
    return channel_data



# Get video data using YouTube API
def get_video_data(channel_id):
    response = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=10
    ).execute()

    video_data = []

    for item in response["items"]:
        if "videoId" in item["id"]:
            video_id = item["id"]["videoId"]
            video_response = youtube.videos().list(
                part="snippet,statistics",
                id=video_id
            ).execute()

            video_data.append(video_response["items"][0])

    return video_data

# Migrate data from MongoDB to MySQL
def migrate_data():
    st.header("Migrate Data")

    # Retrieve channel names for selection
    channel_names = get_channel_names()

    # Append migration options
    migration_options = ["Select a channel to migrate"] + channel_names

    selected_channel = st.selectbox('Select a channel to migrate', migration_options)

    if selected_channel != "Select a channel to migrate":
        channel_data = mongo_db.channels.find_one({"channel_name": selected_channel})
        video_data = mongo_db.videos.find({"channel_name": selected_channel})
        comment_data = mongo_db.comments.find({"channel_name": selected_channel})
        playlist_data = mongo_db.playlists.find({"channel_name": selected_channel})

        # Store channel data in MySQL
        insert_query = "INSERT INTO channels (channel_id, channel_name, subscribers, total_video_count, playlist_id) VALUES (%s, %s, %s, %s, %s)"
        insert_values = (channel_data['channel_id'], channel_data['channel_name'], channel_data['subscribers'], channel_data['total_video_count'], channel_data['playlist_id'])
        mysql_cursor.execute(insert_query, insert_values)

        # Store video data in MySQL
        for video in video_data:
            insert_query = "INSERT INTO videos (video_id, title, likes, dislikes, comments) VALUES (%s, %s, %s, %s, %s)"
            insert_values = (video['video_id'], video['title'], video['likes'], video['dislikes'], video['comments'])
            mysql_cursor.execute(insert_query, insert_values)

        # Store comment data in MySQL
        for comment in comment_data:
            insert_query = "INSERT INTO comments (comment_id, video_id, text, like_count, reply_count, channel_id) VALUES (%s, %s, %s, %s, %s, %s)"
            insert_values = (comment['comment_id'], comment['video_id'], comment['text'], comment['like_count'], comment['reply_count'], comment['channel_id'])
            mysql_cursor.execute(insert_query, insert_values)

        # Store playlist data in MySQL
        for playlist in playlist_data:
            insert_query = "INSERT INTO playlists (playlist_id, title, description, video_count, channel_id) VALUES (%s, %s, %s, %s, %s)"
            insert_values = (playlist['playlist_id'], playlist['title'], playlist['description'], playlist['video_count'], playlist['channel_id'])
            mysql_cursor.execute(insert_query, insert_values)

        mysql_connection.commit()
        st.success("Data migrated to MySQL successfully.")


# Search data in MySQL
def search_data():
    st.header("Search Data")
    search_option = st.selectbox("Search Option", ["Channel Details", "Video Details"])
    
    if search_option == "Channel Details":
        channel_name = st.text_input("Enter Channel Name")
        if st.button("Search"):
            channel_data = search_channel_data(channel_name)
            if channel_data:
                st.write("Channel Name:", channel_data[0])
                st.write("Subscribers:", channel_data[1])
                st.write("Total Videos:", channel_data[2])
            else:
                st.warning("Channel not found.")
    elif search_option == "Video Details":
        video_title = st.text_input("Enter Video Title")
        if st.button("Search"):
            video_data = search_video_data(video_title)
            if video_data:
                st.write("Video ID:", video_data[0])
                st.write("Title:", video_data[1])
                st.write("Likes:", video_data[2])
                st.write("Dislikes:", video_data[3])
                st.write("Comments:", video_data[4])
            else:
                st.warning("Video not found.")

# Search channel data in MySQL
def search_channel_data(channel_name):
    cursor = mysql_connection.cursor()
    sql = "SELECT * FROM channels WHERE channel_name = %s"
    values = (channel_name,)
    cursor.execute(sql, values)
    result = cursor.fetchone()
    cursor.close()
    return result

# Search video data in MySQL
def search_video_data(video_title):
    cursor = mysql_connection.cursor()
    sql = "SELECT * FROM videos WHERE title = %s"
    values = (video_title,)
    cursor.execute(sql, values)
    result = cursor.fetchone()
    cursor.close()
    return result

if __name__ == "__main__":
    main()
