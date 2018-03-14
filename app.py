from elasticsearch import Elasticsearch
from flask import Flask
from ESclient import ESclient
import setting
import json

app = Flask(__name__)

es = ESclient(setting.HOST_ELASTICSEARCH,setting.PORT_ELASTICSEARCH)

@app.route("/hello")
def hello():
    return "hello"

@app.route("/search/<loc>/<param>")
def search(loc,param):
    if loc == "content" :
        rs = search_content(param)
    elif loc == "title" :
        rs = search_title(param)
    elif loc == "url" :
        rs = search_url(param)
    elif loc == "total" :
        rs = search_total(param)
    return rs

def search_content(param):
    query = {
        "query" : {
            "match" : {
                "content" : param
            }
        }
    }
    rs = es.search_data(index="btl",doc_type="work",query=query)
    return json.dumps(rs)


def search_title(param):
    query = {
        "query": {
            "match": {
                "title": param
            }
        }
    }
    rs = es.search_data(index="btl", doc_type="work", query=query)
    return json.dumps(rs)


def search_url(param):
    query = {
        "query": {
            "match": {
                "url": param
            }
        }
    }
    rs = es.search_data(index="btl", doc_type="work", query=query)
    return json.dumps(rs)


def search_total(param):
    pass



if __name__ == "__main__":
    app.run(host=setting.HOST_APP,port=setting.PORT_APP,debug=True)
