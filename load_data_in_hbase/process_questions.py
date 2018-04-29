# import findspark
#
# findspark.init('/home/home/Softwares/spark-2.3.0-bin-hadoop2.7')
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import happybase
from neo4j.v1 import GraphDatabase

server = "localhost"
table_name = "questions"


def remove_html_tags(text):
    # try:
    soup = BeautifulSoup(text, 'html5lib')
    return soup.getText()
    # except:
    #     print(text)


# def remove_bad_record(line):
#     if len(line) == 6:
#         try:
#             val = int(line[0])
#             return True
#         except:
#             print(line)
#             return False
#     else:
#         print(line)
#         return False


def bulk_insert_hbase(batch):
    table = happybase.Connection(server).table(table_name).batch(batch_size=1000)
    for t in batch:
        # try:
        key = t[0]
        value = {"raw:OwnerUserId": t[1],
                 "raw:CreationDate": t[2],
                 "raw:Score": t[3],
                 "raw:Title": t[4],
                 "raw:Body": t[5],
                 "mod:Title": t[6],
                 "mod:Body": t[7]
                 }
        table.put(key, value)
    # table.send()
        # except:
        #     print(t)


class InsertQuestionData(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def save_node(self, id, owner_id, created_dt, score):
        with self._driver.session() as session:
            session.write_transaction(self._create_node_tx, id, owner_id, created_dt, score)

    @staticmethod
    def _create_node_tx(tx, id, owner_id, created_dt, score):
        tx.run("CREATE (q:Question {title:$id, id:$id, owner_id:$owner_id, created_dt:$created_dt,score:$score}) ", id=id, owner_id=owner_id, created_dt=created_dt, score=score)


# def covert_to_int(val):
#     if val == 'NA':
#         return -1
#     else:
#         return int(float(val))


def batch_insert_graph(batch):
    adapator = InsertQuestionData('bolt://localhost:7687', 'neo4j', 'cca')
    for t in batch:
        adapator.save_node(t[0], t[1], t[2], t[3])

    adapator.close()


spark = SparkSession.builder.master("local[*]").appName("CCA") \
    .config("spark.executor.memory", "10gb") \
    .getOrCreate()

# questions_df = pd.read_csv("/home/ubuntu/cca_498_final_project/Questions.csv", encoding='latin1')
questions_df = pd.read_csv("/home/ubuntu/cca_498_final_project/raw_data/local-dev/Questions.csv", encoding='latin1')

questions_df['Id'] = questions_df['Id'].astype(str)
questions_df['OwnerUserId'] = questions_df['OwnerUserId'].astype(str)
questions_df['Score'] = questions_df['Score'].astype(str)

df = spark.createDataFrame(questions_df)

# Remove HTML tags
rdd = df.rdd.map(lambda line: (line[0], line[1], line[2], line[3], line[4], line[5], remove_html_tags(line[4]), remove_html_tags(line[5])))

rdd.foreachPartition(bulk_insert_hbase)

questions_df['Id'] = questions_df['Id'].astype(int)
questions_df['OwnerUserId'] = questions_df['OwnerUserId'].astype(float)
questions_df['Score'] = questions_df['Score'].astype(int)

# rdd.foreachPartition(batch_insert_graph)

spark.stop()
