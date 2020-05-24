import psycopg2
import csv
import sys
from flask import Flask
import simplejson as json
from . import connector

def simpleHello():
    return "Hello and welcome to the PostgreSQL API!"

def getCourses():
    try:
        connection = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "27018",
                                  database = "postgres")

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print ( connection.get_dsn_parameters(),"\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")

    except (Exception) as error :
        print (f"Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            return "Hello World1"
        else:
            return "virker ikke"

def getSingleCourse(id):
    conn = connector.connect2DB()
    cursor = conn.cursor()
    cursor.execute('CALL coursesschema.get_single_client(%s);',[id])
    result = cursor.fetchone()
    print(result)
    return json.dumps({
        'id': result[0],
        'title': result[1],
        'url': result[2],
        'paid': result[3],
        'price': result[4],
        'number_subscribers': result[5],
        'number_reviews': result[6],
        'number_of_lectures': result[7],
        'duration': result[8],
        'level': result[10]        
    })

def postSingleCourse(args):
    return "ses"

def putSingleCourse(args):
    return ""

def deleteSingleCourse(args):
    return ""