from elasticsearch import Elasticsearch,helpers
import logging
import setting
import json

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class ESclient(object):

    def __init__(self,host="localhost",port=1234):
        self.host = host
        self.port = port


    def get_elasticsearch_client(self):
        logging.info("get elasticsearch client")
        return Elasticsearch(
            [{'host': self.host, 'port': self.port}],
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniffer_timeout=60,
            timeout=20
        )


    def read_json(self,filePath):
        logging.info("read json file")
        with open(filePath,"r") as file:
            json_file = file.read()
        return json.loads(json_file)


    def iter_data(self,json_arr):
        for json in json_arr:
            yield json


    def insert_data(self,json_arr,index,doc_type):
        es = self.get_elasticsearch_client()
        helpers.bulk(es,index=index,doc_type=doc_type,actions=self.iter_data(json_arr),stats_only=True)


    def delete_index(self):
        pass


    def reindex_data(self,source_index,target_index,query):
        logging.info("reindex data")
        es = self.get_elasticsearch_client()
        helpers.reindex(es,source_index=source_index, target_index=target_index, query=query)


    def scan_data(self,index,doc_type,query):
        logging.info("scan data from sever")
        es = self.get_elasticsearch_client()
        rs = helpers.scan(es, index=index, doc_type=doc_type, query=query, scroll="1m")
        list_rs = []
        for doc in rs:
            print(doc)
            data = doc["_source"]
            list_rs.append(data)
        for i in list_rs:
            print(i)
        return list_rs


    def count_data(self,index,doc_type,query):
        logging.info("count amount of data")
        es = self.get_elasticsearch_client()
        rs = es.count(index=index, doc_type=doc_type, body=query)
        logging.info("so dong : {}".format(rs["count"]))


    def search_data(self,index,doc_type,query):
        logging.info("search")
        es = self.get_elasticsearch_client()
        rs = es.search(index=index, doc_type=doc_type, body=query,size="1m")
        return rs


    def map_data(self):
        logging.info("map data")
        pass


if __name__ == "__main__":
    pass
    # client = ESclient(setting.HOST_ELASTICSEARCH,setting.PORT_ELASTICSEARCH)
    # client.count_data()
    # client.scan_data(None)
    #json = client.read_json("data/carerrbuilder.json")
    #client.feed_data(json_arr=json,index="btl",doc_type="work")
    # json1 = client.read_json("data/carerrbuilder.json")
    # json2 = client.read_json("data/mywork.json")
    # print(len(json1))
    # print(len(json2))
    # for i in client.iter_data(json1):
    #     print(i)

