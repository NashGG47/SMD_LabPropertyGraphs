import pandas as pd
import numpy as np
import datetime
from uuid import uuid4

VENUES_SOURCE = 'data/semantic_scholar/sc_data_csv/publication-venues-sample.csv'
OUTPUT_PATH_JOURNALS = 'data/semantic_scholar/sc_data_csv/journals.csv'
OUTPUT_PATH_CONFERENCES = 'data/semantic_scholar/sc_data_csv/conferences.csv'
OUTPUT_PATH_IF_FROM = 'data/semantic_scholar/sc_data_csv/is_from.csv'
OUTPUT_PATH_VOLUME_FROM = 'data/semantic_scholar/sc_data_csv/volume_from.csv'

def generate_uuid():
    """Generate a unique identifier."""
    return str(uuid4())

def split_csv():
    venues = pd.read_csv(VENUES_SOURCE)
    journals = venues[venues['type'] == 'journal'][['id', 'name', 'issn', 'url']]
    journals.columns = ['venueID', 'journalName', 'issn', 'url']

    conferences = venues[venues['type'] != 'journal'][['id', 'name', 'issn', 'url']]
    conferences.columns = ['conferenceID', 'conferenceName', 'issn', 'url']
    years = np.arange(2020, 2024)
    volume_from = pd.DataFrame([
        {'journalID': row['venueID'], 'volumeID': generate_uuid(), 'year': year, 'volume': vol+1}
        for _, row in journals.iterrows()
        for year in years
        for vol in range(np.random.randint(1, 3))
    ])
    start_date = datetime.date(1940, 1, 1)
    end_date = datetime.date.today()
    is_from = pd.DataFrame([
        {
            'editionID': generate_uuid(),
            'conferenceID': row['conferenceID'],
            'edition': edition,
            'startDate': start_date + datetime.timedelta(days=np.random.randint(0, (end_date - start_date).days - 30)),
            'endDate': start_date + datetime.timedelta(days=np.random.randint(1, 30))
        }
        for _, row in conferences.iterrows()
        for edition in range(1, np.random.randint(2, 11))
    ])
    journals.to_csv(OUTPUT_PATH_JOURNALS, index=False, encoding='utf-8')
    conferences.to_csv(OUTPUT_PATH_CONFERENCES, index=False, encoding='utf-8')
    volume_from.to_csv(OUTPUT_PATH_VOLUME_FROM, index=False, encoding='utf-8')
    is_from.to_csv(OUTPUT_PATH_IF_FROM, index=False, encoding='utf-8')

