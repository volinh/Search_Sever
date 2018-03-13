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
        """
        :return: trả về 1 instance client kết nói đến server
        """
        logging.info("get elasticsearch client")
        return Elasticsearch(
            [{'host': self.host, 'port': self.port}],
            # sniff_on_start=True,
            # sniff_on_connection_fail=True,
            # sniffer_timeout=60,
            # timeout=20
        )


    def read_json(self,filePath):
        logging.info("read json file")
        with open(filePath,"r") as file:
            json_file = file.read()
        return json.loads(json_file)

    def iter_data(self,json_arr):
        for json in json_arr:
            yield json

    def feed_data(self,body,index,doc_type):
        es = self.get_elasticsearch_client()
        helpers.bulk(es,index=index,doc_type=doc_type,actions=body,refresh = True)

    def delete_index(self):
        pass


if __name__ == "__main__":
    client = ESclient(setting.HOST,setting.HOST)
    json1 = client.read_json("data/carerrbuilder.json")
    client.feed_data(index="btl",doc_type="work",body=client.iter_data(json1))
    # json1 = client.read_json("data/carerrbuilder.json")
    # json2 = client.read_json("data/mywork.json")
    # print(len(json1))
    # print(len(json2))
    # for i in client.iter_data(json1):
    #     print(i)

