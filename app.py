from elasticsearch import Elasticsearch
from flask import Flask

app = Flask(__name__)

@app.route("/hello")
def hello():
    return "hello"


if __name__ == "__main__":
    app.run(host="localhost",port=1234,debug=True)
