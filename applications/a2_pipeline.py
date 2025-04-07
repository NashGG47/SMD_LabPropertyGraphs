from semanthic_scholar.get_data import get_test_data, createSC_to_CSV
from semanthic_scholar.publication_venues_processing import split_csv
from semanthic_scholar.data_relations import generateRelations, generate_synthetic_keywords, extract_topics
from neo4j_pipeline.import_data_toneo4j import importData2Database

def runA2():
    get_test_data()
    createSC_to_CSV()
    split_csv()
    generateRelations()
    generate_synthetic_keywords()
    extract_topics()
    importData2Database()
    print('Semantic scholar data has been processed and relations have been generated.')

if __name__ == '__main__':
    runA2()
    print('Pipeline A2 has been executed successfully.')
