import streamlit as st
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import declarative_base, sessionmaker

api_service_name = "youtube"
api_version = "v3"
api_key = 'YOUR_API_KEY'  # Replace with your API key
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

Base = declarative_base()

class ChannelDetails(Base):
    __tablename__ = 'channel_details'
    Channel_ID = Column(String, primary_key=True)
    Channel_PID = Column(String)
    Channel_Name = Column(String)
    Channel_Description = Column(String)
    Channel_ViewCount = Column(Integer)
    Channel_Subscribers = Column(Integer)
    __table_args__ = (UniqueConstraint('Channel_ID', name='uix_1'),)

class VideoDetails(Base):
    __tablename__ = 'video_details'
    Video_Id = Column(String, primary_key=True)
    Channel_Id = Column(String, ForeignKey('channel_details.Channel_ID'))
    Title = Column(String)
    Tags = Column(String)
    Thumbnail = Column(String)
    Description = Column(String)
    Published_Date = Column(String)
    Duration = Column(String)
    Views = Column(Integer)
    Comments = Column(Integer)
    Favorite_Count = Column(Integer)
    Definition = Column(String)
    Caption_Status = Column(String)
    __table_args__ = (UniqueConstraint('Video_Id', name='uix_2'),)

class CommentDetails(Base):
    __tablename__ = 'comment_details'
    Comment_Id = Column(String, primary_key=True)
    Video_Id = Column(String, ForeignKey('video_details.Video_Id'))
    Author = Column(String)
    Comment_Text = Column(String)
    Published_At = Column(String)
    Like_Count = Column(Integer)
    Reply_Count = Column(Integer)
    __table_args__ = (UniqueConstraint('Comment_Id', name='uix_3'),)

def fetch_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    channel_details = []
    for item in response['items']:
        data = {
            "Channel_ID": item['id'],
            "Channel_PID": item['contentDetails']['relatedPlaylists']['uploads'],
            "Channel_Name": item['snippet']['title'],
            "Channel_Description": item['snippet']['description'],
            "Channel_ViewCount": item['statistics']['viewCount'],
            "Channel_Subscribers": item['statistics']['subscriberCount']
        }
        channel_details.append(data)
    return channel_details

def get_uploads_playlist_id(channel_id):
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return uploads_playlist_id

def get_all_video_ids(playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    while request:
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        request = youtube.playlistItems().list_next(request, response)
    return video_ids

def get_video_details(video_ids):
    video_details = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()
        for item in response['items']:
            video_details.append({
                'Video_Id': item['id'],
                'Channel_Id': item['snippet']['channelId'],
                'Title': item['snippet']['title'],
                'Tags': ','.join(item['snippet'].get('tags', [])),
                'Thumbnail': item['snippet']['thumbnails']['high']['url'],
                'Description': item['snippet']['description'],
                'Published_Date': item['snippet']['publishedAt'],
                'Duration': item['contentDetails']['duration'],
                'Views': item['statistics'].get('viewCount', 0),
                'Comments': item['statistics'].get('commentCount', 0),
                'Favorite_Count': item['statistics'].get('favoriteCount', 0),
                'Definition': item['contentDetails']['definition'],
                'Caption_Status': item['contentDetails']['caption']
            })
    return video_details

def get_comment_details(video_ids):
    comment_details = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            while request:
                response = request.execute()
                for item in response['items']:
                    comment_details.append({
                        'Video_Id': video_id,
                        'Comment_Id': item['id'],
                        'Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_Text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'Published_At': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                        'Like_Count': item['snippet']['topLevelComment']['snippet']['likeCount'],
                        'Reply_Count': item['snippet']['totalReplyCount']
                    })
                request = youtube.commentThreads().list_next(request, response)
        except HttpError as e:
            if e.resp.status == 403 and 'commentsDisabled' in e.content.decode():
                print(f"Comments are disabled for video ID: {video_id}")
            else:
                raise
    return comment_details

def insert_to_postgres(df, table, session):
    if table == ChannelDetails:
        for index, row in df.iterrows():
            exists = session.query(ChannelDetails).filter_by(Channel_ID=row['Channel_ID']).first()
            if not exists:
                session.add(ChannelDetails(**row))
    elif table == VideoDetails:
        for index, row in df.iterrows():
            exists = session.query(VideoDetails).filter_by(Video_Id=row['Video_Id']).first()
            if not exists:
                session.add(VideoDetails(**row))
    elif table == CommentDetails:
        for index, row in df.iterrows():
            exists = session.query(CommentDetails).filter_by(Comment_Id=row['Comment_Id']).first()
            if not exists:
                session.add(CommentDetails(**row))
    session.commit()

def fetch_and_insert_data(channel_id, engine):
    Session = sessionmaker(bind=engine)
    session = Session()

    # Check if the channel already exists
    exists = session.query(ChannelDetails).filter_by(Channel_ID=channel_id).first()
    if exists:
        return None, None, None, True

    channel_info = fetch_channel_data(channel_id)
    channel_df = pd.DataFrame(channel_info)

    uploads_playlist_id = get_uploads_playlist_id(channel_id)
    video_ids = get_all_video_ids(uploads_playlist_id)
    video_details = get_video_details(video_ids)
    comment_details = get_comment_details(video_ids)

    video_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)

    insert_to_postgres(channel_df, ChannelDetails, session)
    insert_to_postgres(video_df, VideoDetails, session)
    insert_to_postgres(comment_df, CommentDetails, session)
    return channel_df, video_df, comment_df, False

def main():
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", ["Insert Data", "View Data", "Query Data"])

    # Database configuration (hidden)
    db_username = 'postgres'
    db_password = 'stream123'
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'postgres'
    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')
    Base.metadata.create_all(engine)

    if selection == "Insert Data":
        st.title("Insert YouTube Data")
        channel_id = st.text_input("Enter YouTube Channel ID")

        if st.button("Insert Data"):
            if channel_id:
                channel_df, video_df, comment_df, exists = fetch_and_insert_data(channel_id, engine)
                if exists:
                    st.warning("This YouTube channel details already exist in the database.")
                else:
                    st.subheader("Channel Data")
                    st.dataframe(channel_df)
                    st.subheader("Video Data")
                    st.dataframe(video_df)
                    st.subheader("Comment Data")
                    st.dataframe(comment_df)
            else:
                st.error("Please enter a YouTube Channel ID")
    elif selection == "View Data":
        st.title("View Data from Database")
        Session = sessionmaker(bind=engine)
        session = Session()

        channels = session.query(ChannelDetails).all()
        channel_options = ["All Channels"] + [channel.Channel_Name for channel in channels]
        selected_channel = st.selectbox("Select Channel", channel_options)

        if st.button("Fetch Data"):
            if selected_channel == "All Channels":
                channel_data = pd.read_sql(session.query(ChannelDetails).statement, session.bind)
                video_data = pd.read_sql(session.query(VideoDetails).statement, session.bind)
                comment_data = pd.read_sql(session.query(CommentDetails).statement, session.bind)
            else:
                channel_id = session.query(ChannelDetails).filter_by(Channel_Name=selected_channel).first().Channel_ID
                channel_data = pd.read_sql(session.query(ChannelDetails).filter_by(Channel_ID=channel_id).statement, session.bind)
                video_data = pd.read_sql(session.query(VideoDetails).filter_by(Channel_Id=channel_id).statement, session.bind)
                comment_data = pd.read_sql(session.query(CommentDetails).join(VideoDetails, VideoDetails.Video_Id == CommentDetails.Video_Id).filter(VideoDetails.Channel_Id == channel_id).statement, session.bind)

            st.subheader("Channel Data")
            st.dataframe(channel_data)
            st.subheader("Video Data")
            st.dataframe(video_data)
            st.subheader("Comment Data")
            st.dataframe(comment_data)
    elif selection == "Query Data":
        st.title("Query Data")

        questions = [
            "1. What are the names of all the videos and their corresponding channels?",
            "2. Which channels have the most number of videos, and how many videos do they have?",
            "3. What are the top 10 most viewed videos and their respective channels?",
            "4. How many comments were made on each video, and what are their corresponding video names?",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
            "8. What are the names of all the channels that have published videos in the year 2022?",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]

        query = st.selectbox("Please select the question", questions)

        Session = sessionmaker(bind=engine)
        session = Session()

        if query == questions[0]:
            videos_and_channels = pd.read_sql(session.query(VideoDetails.Title, ChannelDetails.Channel_Name).join(ChannelDetails).statement, session.bind)
            st.dataframe(videos_and_channels)
        elif query == questions[1]:
            channel_video_count = session.query(ChannelDetails.Channel_Name, func.count(VideoDetails.Video_Id).label('video_count')).join(VideoDetails).group_by(ChannelDetails.Channel_Name).order_by(func.count(VideoDetails.Video_Id).desc()).all()
            df = pd.DataFrame(channel_video_count, columns=['Channel_Name', 'Video_Count'])
            st.dataframe(df)
        elif query == questions[2]:
            top_viewed_videos = session.query(VideoDetails.Title, ChannelDetails.Channel_Name, VideoDetails.Views).join(ChannelDetails).order_by(VideoDetails.Views.desc()).limit(10).all()
            df = pd.DataFrame(top_viewed_videos, columns=['Title', 'Channel_Name', 'Views'])
            st.dataframe(df)
        elif query == questions[3]:
            comments_per_video = pd.read_sql(session.query(VideoDetails.Title, VideoDetails.Comments).statement, session.bind)
            st.dataframe(comments_per_video)
        elif query == questions[4]:
            most_liked_videos = session.query(VideoDetails.Title, ChannelDetails.Channel_Name).join(ChannelDetails).order_by(VideoDetails.Favorite_Count.desc()).all()
            df = pd.DataFrame(most_liked_videos, columns=['Title', 'Channel_Name'])
            st.dataframe(df)
        elif query == questions[5]:
            likes_dislikes = session.query(VideoDetails.Title, VideoDetails.Favorite_Count).all()
            df = pd.DataFrame(likes_dislikes, columns=['Title', 'Favorite_Count'])
            st.dataframe(df)
        elif query == questions[6]:
            total_views_per_channel = session.query(ChannelDetails.Channel_Name, func.sum(VideoDetails.Views).label('total_views')).join(VideoDetails).group_by(ChannelDetails.Channel_Name).all()
            df = pd.DataFrame(total_views_per_channel, columns=['Channel_Name', 'Total_Views'])
            st.dataframe(df)
        elif query == questions[7]:
            channels_2022 = session.query(ChannelDetails.Channel_Name).join(VideoDetails).filter(VideoDetails.Published_Date.like('2022%')).group_by(ChannelDetails.Channel_Name).all()
            df = pd.DataFrame(channels_2022, columns=['Channel_Name'])
            st.dataframe(df)
        elif query == questions[8]:
            avg_duration_per_channel = session.query(ChannelDetails.Channel_Name, func.avg(VideoDetails.Duration).label('avg_duration')).join(VideoDetails).group_by(ChannelDetails.Channel_Name).all()
            df = pd.DataFrame(avg_duration_per_channel, columns=['Channel_Name', 'Avg_Duration'])
            st.dataframe(df)
        elif query == questions[9]:
            most_commented_videos = session.query(VideoDetails.Title, ChannelDetails.Channel_Name).join(ChannelDetails).order_by(VideoDetails.Comments.desc()).all()
            df = pd.DataFrame(most_commented_videos, columns=['Title', 'Channel_Name'])
            st.dataframe(df)

if __name__ == "__main__":
    main()
