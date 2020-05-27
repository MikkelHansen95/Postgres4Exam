import psycopg2
import csv
import sys
from flask import Flask
import simplejson as json
import connector

def simpleHello():
    return "Hello and welcome to the PostgreSQL API!"

def getCourses(args):
    try:
        conn = connector.connect2DB()
        cursor = conn.cursor()
        #GET ALL
        if (args.tag is None and args.level is None and args.price is None and args.comparator is None):
            cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            '''
                           )
        #GET PRICE
        elif (args.tag is None and args.level is None and args.price is not None):
            if(args.comparator is None):
                cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.courses.price = %s''', [args.price]
                           )
            elif('lessThan' in args.comparator):
                cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.courses.price < %s''', [args.price]
                           )
            elif('greaterThan' in args.comparator):
                cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.courses.price > %s''', [args.price]
                           )
         
        #GET LEVEL
        elif (args.tag is None and args.level is not None and args.price is None and args.comparator is None):
            cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.level.title = %s''', [args.level]
                           )
        
        #GET TAG
        elif (args.tag is not None and args.level is None and args.price is None and args.comparator is None):
            cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.subject.title = %s''', [args.tag]
                           )

    except (psycopg2.Error) as error :
        return {'status': 400, 'error': error.pgerror } ,400
    finally:
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append({
                'id': row[0],
                'title': row[1],
                'url': row[2],
                'paid': row[3],
                'price': row[4],
                'number_subscribers': row[5],
                'number_reviews': row[6],
                'number_of_lectures': row[7],
                'duration': json.dumps(row[8]),
                'level': row[10],
                'tags': [ row[11] ]
        
            })
        res = sorted(results,key=lambda x: x['price'], reverse=True)
        return res

def getSingleCourse(id):
    try:
        conn = connector.connect2DB()
        cursor = conn.cursor()
        cursor.execute('''
                            SELECT coursesschema.courses.*, coursesschema.level.title as levelname, coursesschema.subject.title as tag FROM coursesschema.subject 
                            JOIN coursesschema.coursesandsubject on coursesschema.coursesandsubject.subject_id = coursesschema.subject.id
                            JOIN coursesschema.courses on coursesschema.courses.id = coursesschema.coursesandsubject.course_id
                            JOIN coursesschema.level on coursesschema.level.id = courses.level
                            WHERE coursesschema.courses.id = %s''', [id]
                           )
        result = cursor.fetchone()
        #print(result, flush=True)
        if(result is not None):
            return {
                'id': result[0],
                'title': result[1],
                'url': result[2],
                'paid': result[3],
                'price': result[4],
                'number_subscribers': result[5],
                'number_reviews': result[6],
                'number_of_lectures': result[7],
                'duration': json.dumps(result[8]),
                'level': result[10],
                'tags': [ result[11] ]    
        }
        else:
            return {} ,404
    except (psycopg2.Error) as error:
        return {'status': 400, 'error': error.pgerror } ,400
  

def postSingleCourse(args):
    try:
        #print(args, id, flush=True)
        conn = connector.connect2DB()
        cursor = conn.cursor()
        result = cursor.execute('CALL coursesschema.insert_course(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        [ str(args.title), str(args.url), args.paid, args.price, args.number_subscribers, args.number_reviews, args.number_of_lectures, args.level , args.duration ] )
        conn.commit()
        #print(result, flush=True)
        return {'status': 201}, 201
    except (psycopg2.Error) as error:
        #print(f"f.. " , error, flush=True)
        if("23505" in error.pgcode):
            return {'status': 409, 'error': "duplicate key" } ,409
        else:
            return {'status': 400, 'error': error.pgcode}, 400

def putSingleCourse(args,id):
    try:
        #print(args, id, flush=True)
        conn = connector.connect2DB()
        cursor = conn.cursor()
        cursor.execute('CALL coursesschema.update_course(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        [id, args.title, args.url, args.paid, args.price, args.number_subscribers, args.number_reviews, args.number_of_lectures, args.duration, args.level])
        conn.commit()
        #print(result, flush=True)
        return {'status': 204}, 204
    except (psycopg2.Error) as error:
        #print(f"f.. " , error, flush=True)
        if("23505" in error.pgcode):
            return {'status': 409, 'error': "duplicate key" } ,409
        else:
            return {'status': 400, 'error': error.pgerror}, 400
        

def deleteSingleCourse(id):
    try:
        conn = connector.connect2DB()
        cursor = conn.cursor()
        cursor.execute('CALL coursesschema.delete_course(%s)', [id] )
        conn.commit()
        #print(result, flush=True)
        return {'status': 200}
    except (psycopg2.Error) as error:
        #print(f"f.. " , error, flush=True)
        if("23505" in error.pgcode):
            return {'status': 409, 'error': "duplicate key" } ,409
        else:
            return {'status': 400, 'error': error.pgerror}, 400
        