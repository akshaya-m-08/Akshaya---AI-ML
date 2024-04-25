# youtube-data-Harvesting-and-Warehousing-

**Introduction:**

This project entails building a user-friendly Streamlit app that taps into the Google API to gather insightful data from YouTube channels. The collected data is stored in MongoDB and then transferred to a SQL data warehouse for further analysis and exploration, all accessible through the Streamlit interface.

**Technologies Used:**

* Python scripting
* Data Collection
* API integration
* Streamlit
* Data Management using MongoDB,My SQL

**Installation**

To run this project, you need to install the following packages:
   
    import pandas as pd
    from googleapiclient.discovery import build
    import pymysql
    import pymongo
    import streamlit as st
    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np

**Features**

* Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
* Store the retrieved data in a MongoDB.
* Option to check wheather the respective channel Data is exist or Not in MongoDB.
* Migrate the data from MongoDB to a MySQL data warehouse.
* Analyze and visualize data using Streamlit.
* Perform queries on the MySQL data warehouse.
* Display the list of channel name's along with subscription & view's count.

**Retrieving data from the YouTube API**

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments.

**Storing data in MongoDB**

The retrieved data is stored in a MongoDB.
Before storing the data in MongoDB we used the one of the Option to check whether the respective channel Data is exist or Not in MongoDB.

**Migrating data to a MySQL data warehouse**

The application allows users to migrate data from Dataframe to a MySQL data warehouse. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing MySQL queries.

**Analysis**

The project provides comprehensive data analysis capabilities using Plotly and Streamlit. With the integrated Plotly library, users can create interactive and visually appealing charts and graphs to gain insights from the collected data.


**Conclusion**

In summary, this project leverages the Google API to collect, store, and analyze data from YouTube channels, making it accessible through a user-friendly Streamlit application. With MongoDB for initial storage and MySQL for structured data warehousing, users can seamlessly transition and explore data. The integration of Plotly empowers users to create insightful visualizations, enhancing data understanding and enabling data-driven decision-making. This comprehensive approach streamlines the entire process, making it a valuable tool for data enthusiasts and analysts.

**User Interface**


![image]



