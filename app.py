from elasticsearch import Elasticsearch
from flask import Flask
import setting

app = Flask(__name__)

@app.route("/hello")
def hello():
    return "hello"

@app.route("/search/<param>")
def search(param):
    return param

if __name__ == "__main__":
    app.run(host=setting.HOST_APP,port=setting.PORT_APP,debug=True)
