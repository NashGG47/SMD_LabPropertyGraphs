from neo4j_pipeline.import_data_toneo4j import load_all

def runA2_createdb():
    load_all()
    print('Data has been loaded into Neo4j database.')

# if __name__ == '__main__':
#     runA2_createdb()
#     print('Pipeline A2 has been executed successfully.')
