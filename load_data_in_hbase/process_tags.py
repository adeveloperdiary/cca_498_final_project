import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import happybase
from neo4j.v1 import GraphDatabase

server = "hbase-docker"
table_name = "questions"


def bulk_insert_hbase(batch):
    table = happybase.Connection(server).table(table_name)
    for t in batch:
        try:
            key = t[0]
            value = {
                "mod:Tags": ",".join(t[1])
            }
            table.put(key, value)
        except:
            print(t)


class InsertTagNode(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def save_node(self, title):
        with self._driver.session() as session:
            session.write_transaction(self._create_node_tx, title)

    @staticmethod
    def _create_node_tx(tx, title):
        tx.run("CREATE (t:Tag {title:$title}) ", title=title)


class InsertTagData(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def save_node(self, id, tags):
        with self._driver.session() as session:
            session.write_transaction(self._create_node_tx, id, tags)

    @staticmethod
    def _create_node_tx(tx, id, tags):
        for tag in tags:
            try:
                tx.run("MATCH (t:Tag {title: $tag}) "
                       "MATCH (q:Question {id: $id}) "
                       "MERGE (t)-[:TAG_OF]->(q)", id=id, tag=tag.strip().lower())
            except:
                print(tag)
                type(tag)


def covert_to_int(val):
    if val == 'NA':
        return -1
    else:
        return int(float(val))


def batch_insert_graph(batch):
    adapator = InsertTagNode('bolt://localhost:7687', 'neo4j', 'cca')
    for t in batch:
        adapator.save_node(t)
    adapator.close()


def batch_create_edge(batch):
    adapator = InsertTagData('bolt://localhost:7687', 'neo4j', 'cca')
    for t in batch:
        adapator.save_node(covert_to_int(t[0]), t[1])

    adapator.close()


spark = SparkSession.builder.master("local[*]").appName("CCA") \
    .config("spark.debug.maxToStringFields", 999) \
    .config("spark.executor.memory", "40gb") \
    .getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('hdfs://localhost:8020/demo/data/CCA/Tags.csv')

# tags_df = pd.read_csv("/home/ubuntu/cca_498_final_project/raw_data/local-dev/Tags.csv", encoding='latin1')

# rdd = df.rdd.filter(lambda line: remove_bad_record(line=line))

# df = spark.createDataFrame(tags_df)

rdd = df.rdd.groupByKey().mapValues(list)

rdd.foreachPartition(bulk_insert_hbase)

rdd_tags = df.rdd.map(lambda x: (x[1], x[0]))

rdd_tags = rdd_tags.groupByKey()
rdd_tags = rdd_tags.map(lambda x: (x[0]))

rdd_tags.foreachPartition(batch_insert_graph)

rdd.foreachPartition(batch_create_edge)

spark.stop()
