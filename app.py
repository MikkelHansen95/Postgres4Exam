from flask import Flask
import simplejson as json
from flask_restful import reqparse, abort, Api, Resource
import myfunctions
import init
import sys

app = Flask(__name__)
api = Api(app) 

init.populateDB()


class Course(Resource):

    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        #self.reqparse.add_argument('id', type = int, required = True,
        #help = 'No id provided', location = 'args')
        super(Course, self).__init__()

    def get(self, id):
        return myfunctions.getSingleCourse(id)

    def put(self, id):
        self.reqparse.add_argument('title', type = str, required = True,
        help = 'No title provided', location = 'json')
        self.reqparse.add_argument('url', type = str, required = True,
        help = 'No url provided', location = 'json')
        self.reqparse.add_argument('paid', type = bool, required = True,
        help = 'No paid provided', location = 'json')
        self.reqparse.add_argument('price', type = int, required = True,
        help = 'No price provided', location = 'json')
        self.reqparse.add_argument('number_subscribers', type = int, required = True,
        help = 'No number_subscribers provided', location = 'json')
        self.reqparse.add_argument('number_reviews', type = int, required = True,
        help = 'No number_reviews provided', location = 'json')
        self.reqparse.add_argument('number_of_lectures', type = int, required = True,
        help = 'No number_of_lectures provided', location = 'json')
        self.reqparse.add_argument('duration', type = float, required = True,
        help = 'No duration provided', location = 'json')
        self.reqparse.add_argument('level', type = int, required = True,
        help = 'No level provided', location = 'json')
        
        args = self.reqparse.parse_args()
        #print(json.dumps(self.reqparse.parse_args()), flush=True)
        return myfunctions.putSingleCourse(args,id)


    def delete(self,id):
        return myfunctions.deleteSingleCourse(id)

class CoursePost(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        super(CoursePost, self).__init__()

    def post(self):
        self.reqparse.add_argument('title', type = str, required = True,
        help = 'No title provided', location = 'json')
        self.reqparse.add_argument('url', type = str, required = True,
        help = 'No url provided', location = 'json')
        self.reqparse.add_argument('paid', type = bool, required = True,
        help = 'No paid provided', location = 'json')
        self.reqparse.add_argument('price', type = int, required = True,
        help = 'No price provided', location = 'json')
        self.reqparse.add_argument('number_subscribers', type = int, required = True,
        help = 'No number_subscribers provided', location = 'json')
        self.reqparse.add_argument('number_reviews', type = int, required = True,
        help = 'No number_reviews provided', location = 'json')
        self.reqparse.add_argument('number_of_lectures', type = int, required = True,
        help = 'No number_of_lectures provided', location = 'json')
        self.reqparse.add_argument('duration', type = float, required = True,
        help = 'No duration provided', location = 'json')
        self.reqparse.add_argument('level', type = int, required = True,
        help = 'No level provided', location = 'json')
        args = self.reqparse.parse_args()

        return myfunctions.postSingleCourse(args)

class Welcome(Resource):
    def get(self):
        return myfunctions.simpleHello()

class CourseGetAll(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        super(CourseGetAll, self).__init__()
    
    def get(self):
        self.reqparse.add_argument('tag', type = str, required = False,
        help = 'No tag provided', location = 'args')
        self.reqparse.add_argument('level', type = str, required = False,
        help = 'No level provided', location = 'args')
        self.reqparse.add_argument('price', type = int, required = False,
        help = 'No price provided', location = 'args')
        self.reqparse.add_argument('comparator', type = str, required = False,
        help = 'No comparator provided', location = 'args')
        args = self.reqparse.parse_args()
        return myfunctions.getCourses(args)



api.add_resource(Welcome, '/') 
api.add_resource(CourseGetAll, '/courses')
api.add_resource(Course, '/courses/<int:id>')
api.add_resource(CoursePost, '/courses/') 

if __name__ == '__main__':
    app.run(debug=True)