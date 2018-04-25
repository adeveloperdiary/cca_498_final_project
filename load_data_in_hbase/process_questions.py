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


def remove_bad_record(line):
    if len(line) == 6:
        try:
            val = int(line[0])
            return True
        except:
            print(line)
            return False
    else:
        print(line)
        return False


def bulk_insert_hbase(batch):
    # table = happybase.Connection(server).table(table_name)
    for t in batch:
        # try:
        print(t[0])
        key = t[0]
        value = {"raw:OwnerUserId": t[1],
                    "raw:CreationDate": t[2],
                    "raw:Score": t[3],
                    "raw:Title": t[4],
                    "raw:Body": t[5],
                    "mod:Title": t[6],
                    "mod:Body": t[7]
                    }
        # table.put(key, value)
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


def covert_to_int(val):
    if val == 'NA':
        return -1
    else:
        return int(float(val))


def batch_insert_graph(batch):
    adapator = InsertQuestionData('bolt://localhost:7687', 'neo4j', 'cca')
    for t in batch:
        adapator.save_node(covert_to_int(t[0]), covert_to_int(t[1]), t[2], covert_to_int(t[3]))

    adapator.close()


spark = SparkSession.builder.master("local[*]").appName("CCA") \
    .config("spark.executor.memory", "2gb") \
    .getOrCreate()

# df = spark.read.format('csv').option('header', 'true').load('hdfs://localhost:8020/demo/data/CCA/Questions_New.csv')

# questions_df = pd.read_csv("/home/ubuntu/cca_498_final_project/raw_data/local-dev/Questions_New.csv", encoding='latin1')

questions_df = pd.read_csv("/Users/adantonison/workspace/repos/cca_498_final_project/raw_data/local-dev/Questions_New.csv", encoding='latin1')

questions_schema = StructType([StructField('Id', IntegerType(), True),
                               StructField('OwnerUserId', FloatType(), True),
                               StructField('CreationDate', StringType(), True),
                               StructField('Score', IntegerType(), True),
                               StructField('Title', StringType(), True),
                               StructField('Body', StringType(), True)])

# rdd = df.rdd.filter(lambda line: remove_bad_record(line=line))

df = spark.createDataFrame(questions_df, questions_schema)

# Remove HTML tags
rdd = df.rdd.map(lambda line: (line[0], line[1], line[2], line[3], line[4], line[5], remove_html_tags(line[4]), remove_html_tags(line[5])))

print(type(rdd))

rdd.toDf().foreachPartition(bulk_insert_hbase)

# rdd.foreachPartition(batch_insert_graph)

spark.stop()
