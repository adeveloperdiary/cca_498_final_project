from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
from bs4 import BeautifulSoup
import happybase
from neo4j.v1 import GraphDatabase

server = "hbase-docker"
table_name = "answers"


def remove_html_tags(text):
    try:
        soup = BeautifulSoup(text, 'html5lib')
        return soup.getText()
    except:
        print("bs4 issue")
        # print(text)


# def remove_bad_record(line):
#     if len(line) == 6:
#         try:
#             val = int(line[0])
#             return True
#         except:
#             return False
#             print("Bad record")
#             print(line)

#     else:
#         print("Removed bad record")
#         return False


def bulk_insert_hbase(batch):
    table = happybase.Connection(server).table(table_name)
    for t in batch:
        # try:
        key = t[0]
        value = {"raw:OwnerUserId": t[1],
                    "raw:CreationDate": t[2],
                    "raw:ParentId": t[3],
                    "raw:Score": t[4],
                    "raw:Body": t[5],
                    "mod:Body": t[6]
                    }
        table.put(key, value)
        # except:
        #     print("Failed to insert into HBase")
        #     print(t)



class InsertAnswerData(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def save_node(self, id, owner_id, created_dt, parent_id, score):
        with self._driver.session() as session:
            session.write_transaction(self._create_node_tx, id, owner_id, created_dt, parent_id, score)

    @staticmethod
    def _create_node_tx(tx, id, owner_id, created_dt, parent_id, score):
        tx.run("CREATE (a:Answer {title:$id, id:$id, owner_id:$owner_id, created_dt:$created_dt,parent_id:$parent_id,score:$score}) "
               "WITH a "
               "MATCH (q:Question {id: $parent_id}) "
               "MERGE (a)-[:ANS_OF]->(q)", id=id, owner_id=owner_id, created_dt=created_dt,
               parent_id=parent_id, score=score)


# def covert_to_int(val):
#     if val == 'NA':
#         return -1
#     else:
#         return int(float(val))


def batch_insert_graph(batch):
    adapator = InsertAnswerData('bolt://localhost:7687', 'neo4j', 'cca')
    for t in batch:
        # print(t[0])
        adapator.save_node(t[0], t[1], t[2], t[3], t[4])

    adapator.close()


spark = SparkSession.builder.master("local[*]").appName("CCA") \
    .config("spark.executor.memory", "20gb") \
    .getOrCreate()

# df = spark.read.format('csv').option('header', 'true').load('hdfs://localhost:8020/demo/data/CCA/Answers_New.csv')



answers_df = pd.read_csv("/home/ubuntu/cca_498_final_project/raw_data/local-dev/Answers.csv", encoding='latin1')

# answers_df = pd.read_csv("/Users/adantonison/workspace/repos/cca_498_final_project/raw_data/local-dev/Answers_New.csv", encoding='latin1')

answers_df['Id'] = answers_df['Id'].astype(str)
answers_df['OwnerUserId'] = answers_df['OwnerUserId'].astype(str)
answers_df['ParentId'] = answers_df['ParentId'].astype(str)
answers_df['Score'] = answers_df['Score'].astype(str)

# rdd = df.rdd.filter(lambda line: remove_bad_record(line=line))

df = spark.createDataFrame(answers_df)

print(df.printSchema())

# # Remove HTML tags
rdd = df.rdd.map(lambda line: (line[0], line[1], line[2], line[3], line[4], line[5], remove_html_tags(line[5])))

rdd.foreachPartition(bulk_insert_hbase)

answers_df['Id'] = answers_df['Id'].astype(int)
answers_df['OwnerUserId'] = answers_df['OwnerUserId'].astype(float)
answers_df['ParentId'] = answers_df['ParentId'].astype(int)
answers_df['Score'] = answers_df['Score'].astype(int)

df2 = spark.createDataFrame(answers_df)

print(df2.printSchema())

df2.rdd.foreachPartition(batch_insert_graph)

spark.stop()
