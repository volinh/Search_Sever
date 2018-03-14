from elasticsearch import Elasticsearch
from flask import Flask
from ESclient import ESclient
import setting

app = Flask(__name__)

es = ESclient(setting.HOST_ELASTICSEARCH,setting.PORT_ELASTICSEARCH)

@app.route("/hello")
def hello():
    return "hello"

@app.route("/search/<loc>/<param>")
def search(loc,param):
    if loc == "content" :
        search_content(param)
    elif loc == "title" :
        search_title(param)
    elif loc == "url" :
        search_url(param)
    elif loc == "total" :
        search_total(param)


def search_content(param):
    query = {
        "query" : {
            "match" : {
                "content" : param
            }
        }
    }
    rs = es.search_data(index="btl",doc_type="work",query=query)
    return rs


def search_title(param):
    pass


def search_url(param):
    pass


def search_total(param):
    pass



if __name__ == "__main__":
    app.run(host=setting.HOST_APP,port=setting.PORT_APP,debug=True)
