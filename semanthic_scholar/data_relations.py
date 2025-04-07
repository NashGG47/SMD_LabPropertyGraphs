import pandas as pd
import random
import json
from pathlib import Path
import ast

# Paths
BASE_PATH = Path('data/semantic_scholar/sc_data_csv')
INPUT_FILES = {
    'papers-sample': BASE_PATH / 'papers-sample.csv',
    'authors': BASE_PATH / 'authors-sample.csv',
    'editions': BASE_PATH / 'editions.csv',
    'journals': BASE_PATH / 'journals.csv',
    #'keywords': BASE_PATH / 'keywords.csv'
    'abstracts-sample': BASE_PATH / 'abstracts-sample.csv',
}
OUTPUT_FILES = {
    'written_by': BASE_PATH / 'written-by.csv',
    'reviewed_by': BASE_PATH / 'reviewed-by.csv',
    'belongs_to': BASE_PATH / 'belongs-to.csv',
    'published_in': BASE_PATH / 'published-in.csv',
    'papers-sample': BASE_PATH / 'papers-processed.csv',
    'cited_by': BASE_PATH / 'cited-by.csv',
    'related_to': BASE_PATH / 'related-to.csv',
    'with_abstracts': BASE_PATH / 'withAbstracts.csv',
    'affiliated_to': BASE_PATH / 'affiliated-to.csv'
}

def generateRelations():
    data = {key: pd.read_csv(path) for key, path in INPUT_FILES.items()}

    random.seed(123)
    dfs = {
        'belongs_to': pd.DataFrame(columns=['venueID', 'paperID']),
        'published_in': pd.DataFrame(columns=['venueID', 'paperID', 'startPage', 'endPage']),
        'written_by': pd.DataFrame(columns=['paperID', 'authorID', 'is_corresponding']).astype({'is_corresponding': bool}),
        'reviewed_by': pd.DataFrame(columns=['paperID', 'reviewerID', 'grade', 'review']),
        'cited_by': pd.DataFrame(columns=['paperID_cited', 'paperID_citing']),
        'related_to': pd.DataFrame(columns=['paperID', 'keyword']),
        'with_abstracts': pd.DataFrame(columns=['paperID', 'abstract']),
        'affiliated_to': pd.DataFrame(columns=['affiliation', 'authorID', 'type'])
    }
    author_ids = list(data['authors']['authorid'].unique())
    paper_ids = list(data['papers-sample']['corpusid'].unique())
    papers = data['papers-sample']
    colnames = set(json.loads(papers.loc[0, 'externalids'].replace("'", '"').replace("None", '""')).keys()) - {'CorpusId'}
    for col in colnames:
        papers[col] = None
    for index, row in papers.iterrows():
        paper_id = row['corpusid']
        if random.choice([True, False]):
            conf_sample = data['editions'].sample(random.randint(5, 15))
            for _, conf in conf_sample.iterrows():
                dfs['belongs_to'].loc[len(dfs['belongs_to'])] = [conf['editionID'], paper_id]
        else:
            journal_sample = data['journals'].sample(random.randint(5, 15))
            for _, journal in journal_sample.iterrows():
                start, end = random.randint(1, 100), random.randint(101, 200)
                dfs['published_in'].loc[len(dfs['published_in'])] = [journal['venueID'], paper_id, start, end]
        paper_authors = random.sample(author_ids, random.randint(1, 5))
        for i, aid in enumerate(paper_authors):
            dfs['written_by'].loc[len(dfs['written_by'])] = [paper_id, aid, i == 0]
        extids = json.loads(row['externalids'].replace("'", '"').replace("None", '""'))
        extids.pop('CorpusId', None)
        for key, val in extids.items():
            papers.at[index, key] = val
        cited = random.sample([pid for pid in paper_ids if pid != paper_id], random.randint(1, 25))
        for citing in cited:
            dfs['cited_by'].loc[len(dfs['cited_by'])] = [paper_id, citing]
        if index < len(data['abstracts-sample']):
            abstract = data['abstracts-sample'].iloc[index]['abstract']
        else:
            abstract = ""
        dfs['with_abstracts'].loc[len(dfs['with_abstracts'])] = [paper_id, abstract]

   
    for name, df in dfs.items():
        df.to_csv(OUTPUT_FILES[name], encoding='utf-8', index=False)

    papers.to_csv(OUTPUT_FILES['papers-sample'], encoding='utf-8', index=False)


BASE_PATH = Path('data/semantic_scholar/sc_data_csv')
PAPERS_FILE = BASE_PATH / 'papers-processed.csv'
KEYWORDS_FILE = BASE_PATH / 'keywords.csv'
RELATED_TO_FILE = BASE_PATH / 'related-to.csv'
AUTHORS_FILE = BASE_PATH / 'authors-sample.csv'
OUTPUT_FILE_UNIVERSITIES = BASE_PATH / 'universities.csv'
OUTPUT_FILE_COMPANIES = BASE_PATH / 'companies.csv'
OUTPUT_FILE_AFFILIATION = BASE_PATH / 'affiliated-to.csv'
OUTPUT_FILE_REVIEWED_BY= BASE_PATH / 'reviewed-by.csv'


sample_reviews = [
    "This paper provides insightful contributions to the field of artificial intelligence, but it lacks sufficient experimental validation.",
    "An excellent study with great depth and clarity. The methodology is well-designed and results are compelling.",
    "The paper presents a novel approach, but the lack of comprehensive data analysis and comparison with existing techniques limits its impact.",
    "A strong paper with great potential. The results are convincing, but more details on implementation would be helpful.",
    "The research is well-organized, and the arguments are clear. However, it could benefit from a more detailed discussion on future work.",
    "This study offers a solid introduction to the topic but lacks originality and could benefit from a more thorough literature review.",
    "Interesting work. The approach is promising, but the paper would benefit from a more robust evaluation and validation process.",
    "The paper is well-written and presents a clear framework. However, some of the results seem inconclusive and need further clarification."
]


def generate_synthetic_keywords():
    #keywords are not there so we generate them
    papers = pd.read_csv(PAPERS_FILE)
    keyword_pool = [
        'machine learning', 'artificial intelligence', 'deep learning', 'neural networks',
        'data science', 'computer vision', 'natural language processing', 'reinforcement learning',
        'big data', 'robotics', 'internet of things', 'cloud computing', 'data mining', 'pattern recognition'
    ]
    synthetic_keywords = []
    for paper_id in papers['corpusid']:
        num_keywords = random.randint(1, 5)
        assigned_keywords = random.sample(keyword_pool, num_keywords)
        for keyword in assigned_keywords:
            synthetic_keywords.append({'paperID': paper_id, 'keyword': keyword})
    related_to_df = pd.DataFrame(synthetic_keywords)
    related_to_df.to_csv(RELATED_TO_FILE, index=False, encoding='utf-8')

    unique_keywords = list(set(related_to_df['keyword']))
    keywords_df = pd.DataFrame(unique_keywords, columns=['keyword'])
    keywords_df.to_csv(KEYWORDS_FILE, index=False, encoding='utf-8')

    #now for affiliations
    authors = pd.read_csv(AUTHORS_FILE)
    universities = ['University of XYZ', 'Institute of Technology', 'Global University', 'National College']
    companies = ['Company ABC', 'TechCorp Ltd.', 'Innovative Solutions Inc.', 'Startup Hub']
    university_data = []
    company_data = []
    affiliation_data = []
    university_id = 1 
    company_id = 1001 
    for index, row in authors.iterrows():
        author_id = row['authorid']
        affiliations = str(row['affiliations']).split(',') if isinstance(row['affiliations'], str) else []
        if not affiliations:
            affiliations = random.sample(universities + companies, random.randint(1, 2))

        for aff in affiliations:
            if aff in universities:
                university_data.append({'affiliationID': university_id, 'affiliation': aff})
                affiliation_data.append({'authorID': author_id, 'affiliationID': university_id, 'type': 'university'})
                university_id += 1
            elif aff in companies:
                company_data.append({'affiliationID': company_id, 'affiliation': aff})
                affiliation_data.append({'authorID': author_id, 'affiliationID': company_id, 'type': 'company'})
                company_id += 1
    universities_df = pd.DataFrame(university_data)
    companies_df = pd.DataFrame(company_data)
    universities_df.to_csv(OUTPUT_FILE_UNIVERSITIES, index=False, encoding='utf-8')
    companies_df.to_csv(OUTPUT_FILE_COMPANIES, index=False, encoding='utf-8')
    affiliation_df = pd.DataFrame(affiliation_data)
    affiliation_df.to_csv(OUTPUT_FILE_AFFILIATION, index=False, encoding='utf-8')

    #now reviewers
    papers = pd.read_csv(PAPERS_FILE)
    authors = pd.read_csv(AUTHORS_FILE)
    
    review_data = []
    for paper_id in papers['corpusid']:
        num_reviewers = random.randint(1, 3)
        
        for _ in range(num_reviewers):
            reviewer = random.choice(authors['authorid'])
            grade = random.randint(1, 5)
            review = random.choice(sample_reviews)
            review_data.append({
                'paperID': paper_id,
                'reviewerID': reviewer,
                'grade': grade,
                'review': review
            })
    review_df = pd.DataFrame(review_data)
    review_df.to_csv(OUTPUT_FILE_REVIEWED_BY, index=False, encoding='utf-8')

def extract_topics():
    papers_df = pd.read_csv('data/semantic_scholar/sc_data_csv/papers-processed.csv')
    topics_df = papers_df[['s2fieldsofstudy']].copy()
    topics_df['categories'] = topics_df['s2fieldsofstudy'].apply(
        lambda x: ", ".join([field['category'] for field in ast.literal_eval(x)] if pd.notna(x) and x != "" else "")
    )
    topics_df = topics_df[topics_df['categories'] != ""]
    topics_df[['categories']].to_csv('data/semantic_scholar/sc_data_csv/topics.csv', index=False, encoding='utf-8')
