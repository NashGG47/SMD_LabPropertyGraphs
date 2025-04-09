from semanthic_scholar.get_data import get_test_data, createSC_to_CSV
from semanthic_scholar.publication_venues_processing import split_csv
from semanthic_scholar.data_relations import generateRelations, generate_synthetic_keywords, extract_topics, edit_some_editions_volumes, define_topics, create_publish_on

def runA2_load_data():
    get_test_data()
    createSC_to_CSV()
    split_csv()
    generateRelations()
    generate_synthetic_keywords()
    extract_topics()
    edit_some_editions_volumes()
    define_topics()
    create_publish_on()
    print('Semantic scholar data has been processed and relations have been generated.')


if __name__ == '__main__':
    runA2_load_data()
    print('Pipeline A2 has been executed successfully.')
