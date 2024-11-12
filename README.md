
# Automation Scripts for Data Processing and Integration

This repository contains a set of automation scripts designed to streamline data management and integration tasks. The scripts automate processes such as file format conversions (CSV, JSON, XLSX), Google Sheets management, and database operations using MongoDB. They aim to simplify data handling, automate repetitive tasks, and ensure data consistency across different platforms, making it easier for users to manage and process large datasets efficiently.

## Key Features:
* **File Format Conversion** : Automatically convert between different data formats (CSV, JSON, XLSX).
* **Google Sheets Automation**: Automate the creation, updating, and management of Google Sheets using Python.
* **Database Operations**: Automate MongoDB operations including data insertion, updates, and status tracking.
* **Data Integration**: Seamlessly integrate data across systems to ensure consistency and easy retrieval.

## Table of Content
* [Market Data Report Generator](#market_data_reportpy)
* [Record Sync Manager](#record_sync_managerpy)

## market_data_report.py

### Prerequisites
* Python 3.x
* Google Sheets API credentials for pygsheets
* MongoDB server running locally or remotely
* Libraries:
   * pytz
   * pygsheets
   * pymongo
   * logging
   * datetime

Install the necessary Python libraries using:

~~~ 
pip install pygsheets pymongo pytz 
~~~

### Usage

1. Clone this repository or download the script file.
2. Run the script by entering the following command:

    ~~~
    python3 market_data_report.py
    ~~~
3. Enter the MongoDB database as prompted.

### Error Handling
The script includes basic error handling, including:
* Invalid country and frequency choices.
* Missing data handling for specific fields.
* Fallback options if certain competitor data is unavailable.

## record_sync_manager.py
### Prerequisites
* MongoDB - Ensure MongoDB is running and accessible.
* RabbitMQ - Ensure RabbitMQ is running and accessible.
* Python 3.x - The scripts are written in Python 3.

Install the necessary Python dependencies:
```
pip install pymongo pika
```
### Usage

1. Clone this repository or download the script file.
2. Run the script by entering the following command:

    ~~~
    python3 record_sync_manager.py
    ~~~
3. Enter the MongoDB database,Qeue as prompted.

### Error Handling
The script includes basic error handling, including:
* logging.info(): function is imported and used throughout the code to log information and errors.
* File Inserts and Updates: When inserting or updating records in MongoDB, exceptions are captured to handle potential issues like connection errors or document conflicts.
* Detailed Logging: Each error encountered during MongoDB operations is logged.