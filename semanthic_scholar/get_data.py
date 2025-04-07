import os
import pathlib
import requests
import pandas as pd
import gzip
import json
import csv


MANIFEST_URL = "https://s3-us-west-2.amazonaws.com/ai2-s2ag/samples/MANIFEST.txt"
DATASETS_LIST = {'abstracts', 'authors', 'citations', 'paper-ids', 'papers', 'publication-venues'}
DATA_FOLDER = 'data/semantic_scholar/sc_data_1'
OUTPUT_FOLDER = 'data/semantic_scholar/sc_data_csv'

def get_test_data():
    response = requests.get(MANIFEST_URL)
    manifest = response.text.strip().split("\n")

    for file in manifest:
        if ('jsonl' in file and any(word in file for word in DATASETS_LIST)):
            file_url = f"https://s3-us-west-2.amazonaws.com/ai2-s2ag/{file}"
            index = file.rfind('/')
            filename = 'data/semantic_scholar/sc_data_1/' + file[index + 1:]
            print(filename)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "wb") as f:
                response = requests.get(file_url)
                f.write(response.content)


def createSC_to_CSV():
    directory = os.getcwd()
    data_source = directory + '/' + DATA_FOLDER
    directory = os.getcwd()
    pathlib.Path(directory+'/'+OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    for filename in os.listdir(data_source):

        inputfile = DATA_FOLDER + '/' + filename
        outputfile = OUTPUT_FOLDER + '/' + filename.split('.')[0] + '.csv'
        print(outputfile)
        df = pd.read_json(inputfile, lines=True, compression='gzip')
        df.to_csv(outputfile, encoding='utf-8',index=False)
