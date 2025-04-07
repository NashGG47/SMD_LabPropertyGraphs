import pandas as pd
import numpy as np
import json

# File paths
PAPERS_SOURCE = 'data/semantic_scholar/sc_data_csv/papers-sample.csv'
AUTHORS_SOURCE = 'data/semantic_scholar/sc_data_csv/authors-sample.csv'
CONFERENCES_SOURCE = 'data/semantic_scholar/sc_data_csv/is_from.csv'
JOURNALS_SOURCE = 'data/semantic_scholar/sc_data_csv/volume_from.csv'
KEYWORDS_SOURCE = 'data/semantic_scholar/sc_data_csv/keywords.csv'
ABSTRACTS_SOURCE = 'data/semantic_scholar/sc_data_csv/abstracts-sample.csv'
COMPANIES_SOURCE = 'data/semantic_scholar/sc_data_csv/companies.csv'
UNIVERSITIES_SOURCE = 'data/semantic_scholar/sc_data_csv/universities.csv'
REVIEWS_SOURCE = 'data/semantic_scholar/sc_data_csv/reviews.csv'

OUTPUT_FILES = {
    "written_by": "data/semantic_scholar/sc_data_csv/written-by.csv",
    "reviewed_by": "data/semantic_scholar/sc_data_csv/reviewed-by.csv",
    "belongs_to": "data/semantic_scholar/sc_data_csv/belongs-to.csv",
    "published_on": "data/semantic_scholar/sc_data_csv/published-on.csv",
    "papers": "data/semantic_scholar/sc_data_csv/papers-processed.csv",
    "cited_by": "data/semantic_scholar/sc_data_csv/cited-by.csv",
    "related_to": "data/semantic_scholar/sc_data_csv/related-to.csv",
    "withAbstracts": "data/semantic_scholar/sc_data_csv/withAbstracts.csv",
    "affiliated_to": "data/semantic_scholar/sc_data_csv/affiliated-to.csv"
}

def generateRelations():
    np.random.seed(123)
    papers = pd.read_csv(PAPERS_SOURCE)
    authors = pd.read_csv(AUTHORS_SOURCE)
    conferences = pd.read_csv(CONFERENCES_SOURCE)
    journals = pd.read_csv(JOURNALS_SOURCE)
    keywords_df = pd.read_csv(KEYWORDS_SOURCE)
    abstracts = pd.read_csv(ABSTRACTS_SOURCE)
    companies = pd.read_csv(COMPANIES_SOURCE)
    universities = pd.read_csv(UNIVERSITIES_SOURCE)
    reviews = pd.read_csv(REVIEWS_SOURCE)

    authors_ids = authors['authorid'].unique()
    papers_ids = papers['corpusid'].unique()
    keywords_list = keywords_df['keyword'].unique()
    belongs_to_list = []
    published_on_list = []
    written_by_list = []
    reviewed_by_list = []
    cited_by_list = []
    related_to_list = []
    withAbstract_list = []
    affiliated_to_list = []
    def extract_external_ids(row):
        try:
            ext_ids = json.loads(row.replace("'", '"').replace("None", '""'))
            ext_ids.pop("CorpusId", None)
            return ext_ids
        except json.JSONDecodeError:
            return {}

    papers["external_ids_dict"] = papers["externalids"].apply(extract_external_ids)

    for index, row in papers.iterrows():
        paper_id = row['corpusid']
        if np.random.rand() < 0.5:
            conf_sample = conferences.sample(np.random.randint(5, 16))  # 5 to 15
            belongs_to_list.extend([{"venueID": conf["editionID"], "paperID": paper_id} for _, conf in conf_sample.iterrows()])
        else:  # Journal
            journal_sample = journals.sample(np.random.randint(5, 16))  # 5 to 15
            published_on_list.extend([
                {"venueID": journal["volumeID"], "paperID": paper_id, "startPage": start, "endPage": start + np.random.randint(1, 101)}
                for _, journal in journal_sample.iterrows()
                for start in [np.random.randint(1, 101)]
            ])

        #authors
        n_authors = np.random.randint(1, 6)
        selected_authors = np.random.choice(authors_ids, n_authors, replace=False)
        written_by_list.extend([
            {"paperID": paper_id, "authorID": author, "is_corresponding": (i == 0)}
            for i, author in enumerate(selected_authors)
        ])

        #reviewers
        reviewers = np.random.choice([x for x in authors_ids if x not in selected_authors], min(3, len(authors_ids)), replace=False)
        reviewed_by_list.extend([
            {"paperID": paper_id, "reviewerID": reviewer, "grade": np.random.randint(1, 6), "review": reviews.sample(1).iloc[0]["review"]}
            for reviewer in reviewers
        ])
        citing_papers = np.random.choice([x for x in papers_ids if x != paper_id], np.random.randint(1, 26), replace=False)
        cited_by_list.extend([{"paperID_cited": paper_id, "paperID_citing": cite} for cite in citing_papers])

        #keywords
        paper_keywords = np.random.choice(keywords_list, np.random.randint(1, 6), replace=False)
        related_to_list.extend([{"paperID": paper_id, "keyword": kw} for kw in paper_keywords])

        #abstract
        withAbstract_list.append({"paperID": paper_id, "abstract": abstracts.iloc[index]["abstract"]})

        #org
        if np.random.rand() < 0.5:
            affiliation = companies.sample(1).iloc[0]["companyid"]
            aff_type = "company"
        else:
            affiliation = universities.sample(1).iloc[0]["universityid"]
            aff_type = "university"
        affiliated_to_list.append({"affiliation": affiliation, "authorID": selected_authors[0], "type": aff_type})

        for key, value in row["external_ids_dict"].items():
            papers.at[index, key] = value


    belongs_to = pd.DataFrame(belongs_to_list)
    published_on = pd.DataFrame(published_on_list)
    written_by = pd.DataFrame(written_by_list)
    reviewed_by = pd.DataFrame(reviewed_by_list)
    cited_by = pd.DataFrame(cited_by_list)
    related_to = pd.DataFrame(related_to_list)
    withAbstract = pd.DataFrame(withAbstract_list)
    affiliated_to = pd.DataFrame(affiliated_to_list)

    belongs_to.to_csv(OUTPUT_FILES["belongs_to"], index=False)
    published_on.to_csv(OUTPUT_FILES["published_on"], index=False)
    written_by.to_csv(OUTPUT_FILES["written_by"], index=False)
    reviewed_by.to_csv(OUTPUT_FILES["reviewed_by"], index=False)
    papers.to_csv(OUTPUT_FILES["papers"], index=False)
    cited_by.to_csv(OUTPUT_FILES["cited_by"], index=False)
    related_to.to_csv(OUTPUT_FILES["related_to"], index=False)
    withAbstract.to_csv(OUTPUT_FILES["withAbstracts"], index=False)
    affiliated_to.to_csv(OUTPUT_FILES["affiliated_to"], index=False)

