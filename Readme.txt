# YouTube Data Harvesting and Warehousing using Postgres and Streamlit
# Data Collection:

  1.The first step is to collect data from the YouTube using the YouTube Data API. 
  2.The API and the Channel ID is used to retrieve channel details, 
    videos details and comment details. 
  3.I have used the Google API client library for Python to make requests to the API and the responses 
    are Collected as a JSON
  4. Json is converted to dataframe using pandas and dataframes to pushed to the database

# Fetching Data
  1.In a streamlit application , if you select the fetch data, in a drop down you can select the channel name 
    then a selected channel details will be displayed.

# Query Data
  1.In a query data , if a particular question is selected a SQL query is mapped to the question that will fetch the 
    data according to the question. 
