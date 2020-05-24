from flask import Flask, request
from flask_restful import Resource, Api
from . import myfunctions
from . import init
import sys

app = Flask(__name__)
api = Api(app) 
init.populateDB()

class Course(Resource):

    def get(self):
        return myfunctions.getSingleCourse()

    def post(self):
        return myfunctions.postSingleCourse()

    def put(self):
        return myfunctions.putSingleCourse()
    
    def delete(self):
        return myfunctions.deleteSingleCourse()


@app.route("/", methods=['GET'])
def defaultRoute():
    return myfunctions.simpleHello()

@app.route("/courses", methods=['GET'])
def getCoursesApi():
    return myfunctions.getCourses()

@app.route("/courses/<int:key>/", methods=['GET','POST','PUT','POST'])
def singleCourse():
    if (request.method == 'GET'):
        return myfunctions.getSingleCourse(key)
    elif (request.method == 'PUT'):
        return myfunctions.putCourse(key)
    elif (request.method == 'POST'):
        return myfunctions.postCourse(key)
    elif (request.method == 'DELETE'):
        return myfunctions.deleteCourse(key)
    else:
        abort(409)

api.add_resource(Course, '/course/<int:id>') 

if __name__ == '__main__':
    app.run(debug=True)