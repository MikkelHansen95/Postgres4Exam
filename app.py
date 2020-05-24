from flask import Flask, json
from flask_restful import reqparse, abort, Api, Resource
from . import myfunctions
from . import init
import sys

app = Flask(__name__)
api = Api(app) 

init.populateDB()

parser = reqparse.RequestParser()
parser.add_argument('task')


class Course(Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('id', type = int, required = True,
            help = 'No id provided', location = 'json')
        super(Course, self).__init__()

    def get(self, id):
        return myfunctions.getSingleCourse(id)

    def post(self,todo_id):
        #args = parser.parse_args()
        #return myfunctions.postSingleCourse(args)
        return "ses"

    def put(self,todo_id):
        #args = parser.parse_args()
        #return myfunctions.putSingleCourse(args)
        return ""
    def delete(self,todo_id):
        #args = parser.parse_args()
        #return myfunctions.deleteSingleCourse(args)
        return ""

class Welcome(Resource):
    def get(self):
        return myfunctions.simpleHello()


api.add_resource(Welcome, '/') 
api.add_resource(Course, '/course/<int:id>', endpoint = 'course') 

if __name__ == '__main__':
    app.run(debug=True)