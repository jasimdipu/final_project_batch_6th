from flask import Flask
from test.test import TestClass, Profile

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/my_name')
def get_name():
    return "Md Jasim Uddin Dipu"


@app.route("/get_num")
def get_number():
    num = TestClass(100)
    number_str = str(num.get_num())
    return number_str


@app.route("/get-profile")
def get_profile():
    profile = Profile("Md Sajib Mia", "Dhaka")
    pr = str(profile)
    return pr


if __name__ == '__main__':
    app.run()
