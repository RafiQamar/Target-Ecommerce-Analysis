import pandas as pd
import datetime
import os
from sqlalchemy import create_engine
'''SQLAlchemy is used as the ORM (Object Relational Mapper) or a high-level database toolkit.
It abstracts away the complexity of raw SQL queries, allowing you to interact with databases using Python 
objects and methods.
In this code, it provides the connection engine (create_engine) for interacting with the database and 
facilitates the insertion of Pandas DataFrames (data.to_sql).
Without SQLAlchemy
* Requires more manual coding for SQL statements.
* No built-in Pandas integration (like to_sql).
* No ORM features (e.g., automatic table creation).'''
# !pip install pymysql
'''PyMySQL is a pure-Python MySQL client library, designed to let you connect to and 
interact with MySQL databases directly from your Python applications'''
# Record start time
start_time = datetime.datetime.now()
print('Begin:', start_time)

# Counter for imported files
num = 0
'''This string is a SQLAlchemy connection URL that connects to a MySQL database using the pymysql driver.'''
# Create the SQLAlchemy engine for MySQL connection containig username, password, hostname, portnumber, database
engine = create_engine('mysql+pymysql://root:Rafi%40mysql123@localhost:3306/commerce?charset=utf8mb4')
"""MySQL uses utf8mb4 as its full UTF-8 implementation, supporting a broader range of characters.
If your file has a different encoding, you should convert it to UTF-8 to avoid issues during import."""

# Path to the folder containing CSV files
path = r'C:\Users\rafiq\Desktop\Project\Target Sales Dataset'

# List all files in the folder
files = os.listdir(path)

# Iterate over all files
for i in files:
    # Construct the full file path
    file_path = os.path.join(path, i)
    
    # Check if the file is a CSV file
    if i.endswith('.csv'):
        # Extract the table name from the file name (remove extension)
        table_name = os.path.splitext(i)[0]
        
        try:
            # Read the CSV file into a pandas DataFrame
            data = pd.read_csv(file_path, header=0)
            """In pandas.read_csv(), the header parameter defaults to 0, meaning the first row of the CSV file is 
            treated as the column headers by default. Therefore, explicitly specifying header=0 is redundant unless 
            you want to emphasize the behavior for clarity."""
            # Import the data into MySQL
            data.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
            """ to_sql(): Inserts the DataFrame into the MySQL table.
                name=table_name: Specifies the target table name.
                con=engine: Uses the SQLAlchemy engine to connect to MySQL.
                index=False: Excludes the DataFrame index column from being inserted.
                if_exists='replace': Drops the table if it exists and creates a new one."""
            num += 1
            print(f"Successfully imported: {i} into table {table_name}")
        except Exception as e:
            print(f"Failed to import: {i}. Error: {e}")
            """Catches and displays any errors that occur during the import process, such as file format issues 
            or database connection problems."""

# Record end time
end_time = datetime.datetime.now()
print('End:', end_time)

# Calculate and display total time taken
total_time = end_time - start_time
print('Total time:', total_time)

# Display the total number of imported files
print('Total number of imported files:', num)
