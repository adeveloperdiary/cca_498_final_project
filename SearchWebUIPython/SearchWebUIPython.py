from flask import Flask, request, jsonify, send_from_directory
import happybase
import textwrap
from neo4j.v1 import GraphDatabase

app = Flask(__name__)


class GetAnswerCount(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def get_answers_count(self, id):
        with self._driver.session() as session:
            return session.write_transaction(self._get_answers_count, id)

    @staticmethod
    def _get_answers_count(tx, id):
        result = tx.run("MATCH (q:Question {id: $id}) MATCH (a)-[:ANS_OF]->(q) RETURN count(a)", id=int(id))
        count = 0
        for record in result:
            count = record["count(a)"]
        return count


def batch_insert_graph(id):
    adapator = GetAnswerCount('bolt://192.168.50.57:7687', 'neo4j', 'cca')
    count = adapator.get_answers_count(id)

    adapator.close()
    return count


@app.route('/get_random_questions_from_hbase', methods=['GET'])
def get_random_questions_from_hbase():
    connnection = happybase.Connection("192.168.50.57")
    table = connnection.table("questions")
    rows = table.rows([b'35486227', b'14853757', b'19170111', b'23059398', b'26580944', b'29748897', b'32765572', b'38056227', b'4590516', b'9998815'])

    connnection.close()

    row_data = {}

    for key, data in rows:
        try:
            if b'mod:Tags' in data:
                row_data[key.decode("utf-8")] = ({'body': data[b'raw:Body'].decode("utf-8").replace("\"", ""),
                                                  'title': data[b'mod:Title'].decode("utf-8"),
                                                  'date': data[b'raw:CreationDate'].decode("utf-8"),
                                                  'ownerid': data[b'raw:OwnerUserId'].decode("utf-8"),
                                                  'score': data[b'raw:Score'].decode("utf-8"),
                                                  'tags': str(data[b'mod:Tags'].decode("utf-8")).split(','),
                                                  'count': batch_insert_graph(key.decode("utf-8"))
                                                  })
            else:
                row_data[key.decode("utf-8")] = ({'body': textwrap.shorten(data[b'raw:Body'].decode("utf-8").replace("\"", ""), width=500, placeholder="..."),
                                                  'title': data[b'mod:Title'].decode("utf-8"),
                                                  'date': data[b'raw:CreationDate'].decode("utf-8"),
                                                  'ownerid': data[b'raw:OwnerUserId'].decode("utf-8"),
                                                  'score': data[b'raw:Score'].decode("utf-8"),
                                                  'tags': [],
                                                  'count': batch_insert_graph(key.decode("utf-8"))
                                                  })

        except:
            print("Error")

    return jsonify(row_data)


if __name__ == '__main__':
    app.run()