from neo4j_pipeline.a3_load_data import a3_load_all

def runA3_load():
    a3_load_all()
    print('Data has been loaded into Neo4j database.')

# if __name__ == '__main__':
#     runA3_load()
#     print('Pipeline A2 has been executed successfully.')
