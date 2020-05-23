from flask import Flask, jsonify, request
from . import myfunctions

app = Flask(__name__)

myfunctions.populateCourses()

@app.route("/", methods=['GET','POST'])
def defaultRoute():
    return myfunctions.simpleHello()

@app.route("/courses", methods=['GET','POST'])
def getCoursesApi():
    return myfunctions.getCourses()


if __name__ == '__main__':
    app.run(debug=True)