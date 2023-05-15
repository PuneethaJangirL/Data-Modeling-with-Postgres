# Analysis of Songs Data

This project was designed to analyze what kind of songs different users are listening to or the so called **'Song Play Analysis'** for a startup called **Sparkify**. The main goal is compare the results of the analysis team with the results in this project. It involves data modeling with Postgres and builing an ETL pipeline using Python. The raw data in JSON format is transformed to the Postgres database using Python libraries and SQL.

## Getting Started

### Prerequisites

- **Python3**

Install python3 using the below code for Windows or Mac OS.

> {'sudo apt-get install python', 'brew install python'}

- **Code editor like VS Code or Jupiter Notebook**

    - Jupiter notebook can be installed using Anaconda or pip
    
> {'sudo apt-get install code', 'python -m pip install --upgrade pip'}

### Running Python Scripts

The code below can be used to run the python scripts in the terminal for Python version 3 and above.

> python3 file_name.py

## Roadmap

1. data/

This folder contains of metadata about songs and logs of users in JSON format.

2. sql_queries.py

Includes the Database schema, creation and deletion of all tables.

3. create_tables.py

Connection to the Sparkify database and access to sql_queries.py in Python using the **psycopg2** library. Reset all tables in the database.

4. etl.ipynb

Jupiter notebook to perform translation from JSON data to database schema (STAR schema) specified in sql_queries.py

Processes individual song and log data into tables.

5. etl.py

Processes all the song data and log data and loads into the database.

6. test.ipynd

Includes basic Sanity tests for the database schema like primary key constraints, not null constraints and others.

## Database Schema 

Star schema was used to implement the database due its simplicity and less execution time for queries. Although it has high data redundancy the interdependency between tables is reduced, thereby reducing the query complexity.

## ETL Pipeline

The **psycopg2** module from Python was used to extract, transform and load the data in JSON format to SQL as it has high code readabilty and performance.

## License

This project is licensed under Sparkify License



