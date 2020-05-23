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

    except (Exception, psycopg2.Error) as error :
        print (f"Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            return "Hello World1"
        else:
            return "virker ikke"

def createDBStructure():
    try:
        create_schema = '''
        CREATE SCHEMA IF NOT EXISTS coursesschema;
        '''

        create_table_courses_level_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.courses_level (
            id serial PRIMARY KEY,
            title varchar(50) NOT NULL,
            description varchar(150) NOT NULL,
            price_per_lecture int NOT NULL
        )
        '''
        create_table_courses_subject_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.subjects (
            id serial PRIMARY KEY,
            title varchar(50) NOT NULL,
            description varchar(150) NOT NULL
        )
        '''

        create_table_courses_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.courses (
            id serial PRIMARY KEY,
            title varchar(60) NOT NULL,
            url varchar(200) NOT NULL,
            paid boolean NOT NULL DEFAULT false,
            price int NOT NULL,
            number_subscribers int NOT NULL,
            number_reviews int NOT NULL,
            level int REFERENCES coursesschema.courses_level(id)
        )
        '''

        create_table_coursesXsubjects_query = '''
		CREATE TABLE IF NOT EXISTS coursesschema.courseXlevel (
            course_id int REFERENCES coursesschema.courses(id),
            subject_id int REFERENCES coursesschema.subjects(id),
            PRIMARY KEY (course_id,subject_id)
        )
        '''

        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute(create_schema)
        connection.commit()
        cursor.execute(create_table_courses_level_query)
        connection.commit()
        cursor.execute(create_table_courses_subject_query)
        connection.commit()
        cursor.execute(create_table_courses_query)
        connection.commit()
        cursor.execute(create_table_coursesXsubjects_query)
        connection.commit()
    except (Exception) as error :
        print (f"Couldn't open and populate db from file", error, file=sys.stderr)
    finally:
        cursor.close()
        connection.close()    

def populateCourses():
    try:
        createDBStructure()
        
        #with open('udemy_courses.csv',newline='') as csvfile:
            #reader = csv.reader(csvfile,delimiter=',', quotechar='"')
            #for row in reader:
                #print(",".join())

    except (Exception) as error :
        print (f"Couldn't open and populate db from file", error, file=sys.stderr)
    finally:
        print("works",file=sys.stderr)
        #cursor.close()
        #connection.close()

def connect2DB():
    return psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "27018",
                                  database = "postgres")
