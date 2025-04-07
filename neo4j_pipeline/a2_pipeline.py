from semanthic_scholar.get_data import get_test_data, createSC_to_CSV
from semanthic_scholar.publication_venues_processing import split_csv
from semanthic_scholar.data_relations import generateRelations

def runA2():
    get_test_data()
    createSC_to_CSV()
    split_csv()
    generateRelations()
    print('Semantic scholar data has been processed and relations have been generated.')

if __name__ == '__main__':
    runA2()
    print('Pipeline A2 has been executed successfully.')
