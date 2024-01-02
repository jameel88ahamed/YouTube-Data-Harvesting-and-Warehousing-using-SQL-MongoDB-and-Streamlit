# YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit
Youtube-Data-Harvesting-And-Warehousing YouTube Data Harvesting and Warehousing is a project that intends to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. Extracting data using Youtube API and storing it on MongoDB then Transforming it to a relational databaselike PostgreSQL. For getting various info about youtube channels.

TOOLS AND LIBRARIES USED: this project requires the following components:

STREAMLIT: Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

PYTHON: Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

GOOGLE API CLIENT: The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

MONGODB: MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

POSTGRESQL: PostgreSQL is an open-source, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features. It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.
REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlit

3.psycopg2

4.pymongo

5.pandas

## **Workflow :**

* Connect to the YouTube API this API is used to retrieve channel, videos, comments data. I have used the Google API client library for Python to make requests to the API.

* The user will able to extract the Youtube channel's data using the Channel ID. Once the channel id is provided the data will be extracted using the API.

* Once the data is retrieved from the YouTube API, I've stored it in a MongoDB as data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.

* After collected data for multiple channels,it is then migrated/transformed it to a structured PostgreSQL as data warehouse.

* Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.

* With the help of SQL query I have got many interesting insights about the youtube channels.

