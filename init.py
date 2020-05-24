import psycopg2
import csv
import sys

def createDBStructure():
    try:
        create_schema = '''
        CREATE SCHEMA IF NOT EXISTS coursesschema;
        '''

        create_table_courses_level_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.level (
            id serial PRIMARY KEY,
            title text UNIQUE NOT NULL,
            description text NOT NULL
        )
        '''
        create_table_courses_subject_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.subject (
            id serial PRIMARY KEY,
            title text UNIQUE NOT NULL,
            description text NOT NULL
        )
        '''

        create_table_courses_query = '''
        CREATE TABLE IF NOT EXISTS coursesschema.courses (
            id serial PRIMARY KEY,
            title text NOT NULL,
            url text UNIQUE NOT NULL,
            paid boolean NOT NULL DEFAULT false,
            price int NOT NULL,
            number_subscribers int NOT NULL,
            number_reviews int NOT NULL,
            number_of_lectures int NOT NULL,
            duration numeric NOT NULL,
            level int REFERENCES coursesschema.level(id)
        )
        '''

        create_table_coursesAndsubject_query = '''
		CREATE TABLE IF NOT EXISTS coursesschema.coursesandsubject (
            id serial PRIMARY KEY NOT NULL,
            course_id int REFERENCES coursesschema.courses(id) NOT NULL,
            subject_id int REFERENCES coursesschema.subject(id) NOT NULL
        )
        '''

        create_table_log_query = '''
		CREATE TABLE IF NOT EXISTS coursesschema.log (
            id serial PRIMARY KEY,
            logmsg varchar(999) NOT NULL
        )
        '''

        storedprod_insert_level = '''
        CREATE OR REPLACE PROCEDURE coursesschema.insert_level(text, text)
        LANGUAGE plpgsql
	    AS $$
        BEGIN
	        INSERT INTO coursesschema.level (title, description) VALUES ($1,$2);
        END;
	    $$;
        '''
        storedprod_insert_subject = '''
        CREATE OR REPLACE PROCEDURE coursesschema.insert_subject(text, text)
        LANGUAGE plpgsql
	    AS $$
        BEGIN
	        INSERT INTO coursesschema.subject (title, description) VALUES ($1,$2);
        END;
	    $$;
        '''
        storedprod_insert_course = '''
        CREATE OR REPLACE PROCEDURE coursesschema.insert_course(text, text, boolean, int, int, int, int, int, numeric)
        LANGUAGE plpgsql
	    AS $$
        BEGIN
	        INSERT INTO coursesschema.courses 
            (title, url, paid, price, number_subscribers, number_reviews, number_of_lectures, level, duration)
            VALUES 
            ($1,$2,$3,$4,$5,$6,$7,$8,$9);
        END;
	    $$;
        '''
        storedprod_insert_courseWithSubject = '''
        CREATE OR REPLACE PROCEDURE coursesschema.insert_course_with_subject(int, int)
        LANGUAGE plpgsql
	    AS $$
        BEGIN
	        INSERT INTO coursesschema.coursesandsubject 
            (course_id, subject_id)
            VALUES 
            ($1,$2);
        END;
	    $$;
        '''
        storedprod_insert_log = '''
        CREATE OR REPLACE PROCEDURE coursesschema.insert_log(text)
        LANGUAGE plpgsql
	    AS $$
        BEGIN
	        INSERT INTO coursesschema.log 
            (logmsg)
            VALUES 
            ($1);
        END;
	    $$;
        '''
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute(create_schema)
        cursor.execute(create_table_courses_level_query)
        cursor.execute(create_table_courses_subject_query)
        cursor.execute(create_table_courses_query)
        cursor.execute(create_table_coursesAndsubject_query)
        cursor.execute(create_table_log_query)
        cursor.execute(storedprod_insert_level)
        cursor.execute(storedprod_insert_subject)
        cursor.execute(storedprod_insert_course)
        cursor.execute(storedprod_insert_courseWithSubject)
        cursor.execute(storedprod_insert_log)
        connection.commit()
    except (psycopg2.Error) as error:
        connection.rollback()
        print (f"Couldn't open and populate db from file", error, file=sys.stdout)
    finally:
        cursor.close()
        connection.close()    

def populateDB():
    try:
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'coursesschema';")
        result = cursor.fetchone()
        if(result is not None):
            return
        createDBStructure()
        levelArray = []
        subjectArray = []
        courseUrlArray = []
       
        with open('udemy_courses.csv', encoding='utf-8', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader)
            price = 50
            for row in reader:
                if(row[8] not in levelArray):
                    price += 50
                    print( levelArray.append(row[8]), file=sys.stdout )
                    INIT_insertCourseLevel(row[8])
                if(row[11] not in subjectArray):
                    subjectArray.append(row[11])
                    INIT_insertSubject(row[11])

        with open('udemy_courses.csv', encoding='utf-8', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader)
            price = 50
            for row in reader:
                if(row[11] == ""):
                    continue
                else:
                    courseUrlArray.append(row[2])
                    INIT_insertCourse(row,levelArray)
                    INIT_insertCoursesAndSubject(row,subjectArray,courseUrlArray)

    except (psycopg2.Error) as error:
        #connection.rollback()
        insertLog(error)
        print (f"failed to init DB: ", error, flush=True)



def INIT_insertCourseLevel(level):
    try:
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute('CALL coursesschema.insert_level(%s,%s);', (level, "Generic description") )
        connection.commit()
    except (psycopg2.Error) as error:
        #Checking for duplicate unique keys
        if("23505" not in error.pgcode):
            print (f"ERROR: insert course level ", error, flush=True)
            connection.rollback()
        insertLog(error)
    finally:
        cursor.close()
        connection.close()


def INIT_insertSubject(subject):
    try:
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute('CALL coursesschema.insert_subject(%s,%s)',(subject,"Some description about a subject"))
        connection.commit()
    except (psycopg2.Error) as error:
        #Checking for duplicate unique keys
        if("23505" not in error.pgcode):
            print (f"ERROR: insert subject  ",  error, flush=True)
            connection.rollback()
        insertLog(error)
    finally:
        cursor.close()
        connection.close()

def INIT_insertCourse(row,levelArr):
    try:
        index = levelArr.index(row[8])
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute('CALL coursesschema.insert_course(%s,%s,%s,%s,%s,%s,%s,%s,%s)', [row[1], row[2], row[3], row[4], row[5], row[6], row[7], index+1, row[9] ] )
        connection.commit()
    except (psycopg2.Error) as error:
        #Checking for duplicate unique keys
        if("23505" not in error.pgcode):
            print (f"ERROR: insert course  ",  error, flush=True)
            connection.rollback()
        insertLog(error)
    finally:
        cursor.close()
        connection.close()

def INIT_insertCoursesAndSubject(row,subjectArr,courseUrlArr):
    try:
        indexSubject = subjectArr.index(row[11])
        indexCourseUrl = courseUrlArr.index(row[2])
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute('CALL coursesschema.insert_course_with_subject(%s,%s)',[indexCourseUrl+1, indexSubject+1])
        connection.commit()
    except (psycopg2.Error) as error:
        #Checking for duplicate unique keys
        if("23505" not in error.pgcode):
            print (f"ERROR: insert course  ",  error, flush=True)
            connection.rollback()
        insertLog(error)
    finally:
        cursor.close()
        connection.close()

def insertLog(error):
    try:
        #record_to_insert = [error.pgerror]
        connection = connect2DB()
        cursor = connection.cursor()
        cursor.execute("CALL coursesschema.insert_log(%s)", [error.pgerror])
        connection.commit()
    except (psycopg2.Error) as error:
        print (f"LOG ERROR: ", error, file=sys.stderr)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def connect2DB():
    return psycopg2.connect(    user = "postgres",
                                password = "password",
                                host = "127.0.0.1",
                                port = "27018",
                                database = "postgres")
