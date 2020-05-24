import psycopg2
import csv
import sys

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

def getSingleCourse():
    return ""

def postSingleCourse():
    return ""

def putSingleCourse():
    return ""

def deleteSingleCourse():
    return ""