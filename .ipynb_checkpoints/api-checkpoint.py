# API to return data from the PostgreSQL server

from flask import Flask, request
import psycopg2
from dotenv import load_dotenv

# Creates an instance of the Flask app
app = Flask(__name__)

@app.get("/people")
def get_people():
    '''
    Recieves a date and returns all the people where its inclusion date is equal to it.

    Args:
        date (date): the inclusion date for a person

    Returns:
        JSON: List of people with the inclusion date equals to the date recieved
    '''
    
    
    # Get the data recieved from the request
    date = request.args.get('date')

    # Loads the environmental variables. Secrets that are stored in my machine.
    load_dotenv()
    sqluser = os.getenv("SQLUSER")
    sqlpass = os.getenv("SQLPASS")
    
    # Gets server data
    host = '127.0.0.1'
    database = 'etl_p1'
    
    # Creates the path to the database
    conn_string = f'postgresql://{sqluser}:{sqlpass}@{host}/{database}'

    # Opens a connection to the server
    conn = psycopg2.connect(conn_string) 
    conn.autocommit = True

    # Creates the cursor to run queries
    cursor = conn.cursor() 

    # Runs a query filtering by the date recieved from the client
    query = f'select * from person where inclusion_date = {date};'
    cursor.execute(query) 

    # Creates a list of people
    people = []

    # Initializes the name of the columns
    col_names = [ 
        'first_name', 
        'last_name', 
        'age', 
        'height', 
        'weight', 
        'p_type', 
        'status', 
        'op2_level', 
        'inclusion_date'
    ]

    # For each value recieved in the query creates a dictionary to encapsulate all the data for each person.
    for row in cursor.fetchall():
        person = {'id': row[0]}
        attributes = {}
        
        for i in range(len(col_names)):
            attributes[col_names[i]] = row[i + 1]

        person['attributes'] = attributes
        people.append(person) 

    # Returns the dictionary as a JSON file to the client containing all the people with inclusio date equals to the date recieved as an argument
    return people