import pandas as pd
# --- Neo4j Loading ---
def run_query(tx, query, parameters=None):
    tx.run(query, parameters or {})
    
papers = pd.read_csv("data/semantic_scholar/sc_data_csv/papers-processed.csv")
authors = pd.read_csv("data/semantic_scholar/sc_data_csv/authors-sample.csv")
authors = authors.fillna('')
topics = pd.read_csv("data/semantic_scholar/sc_data_csv/topics.csv")
reviewers = pd.read_csv("data/semantic_scholar/sc_data_csv/reviewed-by.csv")
journals = pd.read_csv("data/semantic_scholar/sc_data_csv/journals.csv")
conferences = pd.read_csv("data/semantic_scholar/sc_data_csv/conferences.csv")
volumes = pd.read_csv("data/semantic_scholar/sc_data_csv/volume.csv")
is_from = pd.read_csv("data/semantic_scholar/sc_data_csv/is_from.csv")
volume_from = pd.read_csv("data/semantic_scholar/sc_data_csv/volume_from.csv")
editions = pd.read_csv("data/semantic_scholar/sc_data_csv/editions.csv")
keywords = pd.read_csv("data/semantic_scholar/sc_data_csv/keywords.csv")
related_to = pd.read_csv('data/semantic_scholar/sc_data_csv/related-to.csv')
written_by = pd.read_csv('data/semantic_scholar/sc_data_csv/written-by.csv')
reviewed_by = pd.read_csv('data/semantic_scholar/sc_data_csv/reviewed-by.csv')
cited_by = pd.read_csv('data/semantic_scholar/sc_data_csv/cited-by.csv')

from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import json
import os
import sys
def load_all():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Constraints
        # Nodes
        # Load papers
        for _, row in papers.iterrows():
            paper_data = {
                "id": str(row.get("corpusid", "")),
                "externalids": str(row.get("externalids", "")),
                "url": row.get("url", ""),
                "title": row.get("title", ""),
                "year": int(row.get("year", 0)) if not pd.isna(row.get("year")) else 0,
                "referencecount": int(row.get("referencecount", 0)),
                "citationcount": int(row.get("citationcount", 0)),
                "influentialcitationcount": int(row.get("influentialcitationcount", 0)),
                "isopenaccess": bool(row.get("isopenaccess", False)),
                "s2fieldsofstudy": str(row.get("s2fieldsofstudy", "")),
                "publicationtypes": str(row.get("publicationtypes", "")),
                "publicationdate": row.get("publicationdate", ""),
                "ArXiv": row.get("ArXiv", ""),
                "MAG": row.get("MAG", ""),
                "ACL": row.get("ACL", ""),
                "DBLP": row.get("DBLP", ""),
                "DOI": row.get("DOI", ""),
                "PubMedCentral": row.get("PubMedCentral", ""),
                "PubMed": row.get("PubMed", "")
            }

            label_query = """
                MERGE (p:Paper {id: $id})
                SET p.externalids = $externalids,
                    p.url = $url,
                    p.title = $title,
                    p.year = $year,
                    p.referencecount = $referencecount,
                    p.citationcount = $citationcount,
                    p.influentialcitationcount = $influentialcitationcount,
                    p.isopenaccess = $isopenaccess,
                    p.s2fieldsofstudy = $s2fieldsofstudy,
                    p.publicationtypes = $publicationtypes,
                    p.publicationdate = $publicationdate,
                    p.Arxiv = $ArXiv,
                    p.MAG = $MAG,
                    p.ACL = $ACL,
                    p.DBLP = $DBLP,
                    p.doi = $DOI,
                    p.PubMedCentral = $PubMedCentral,
                    p.PubMed = $PubMed
            """

            session.execute_write(run_query, label_query, paper_data)
        # Load authors
        for _, row in authors.iterrows():
            author_data = {
                "authorid": str(row.get("authorid", "")),
                "externalids": str(row.get("externalids", "")),
                "url": row.get("url", ""),
                "name": row.get("name", ""),
                "aliases": str(row.get("aliases", "")),
                "homepage": row.get("homepage", ""),
                "papercount": int(row.get("papercount", 0)),
                "citationcount": int(row.get("citationcount", 0)),
                "hindex": int(row.get("hindex", 0))
            }

            label_query = """
                MERGE (a:Author {authorid: $authorid})
                SET a.name = $name,
                    a.externalids = $externalids,
                    a.url = $url,
                    a.aliases = $aliases,
                    a.homepage = $homepage,
                    a.papercount = $papercount,
                    a.citationcount = $citationcount,
                    a.hindex = $hindex
            """

            session.execute_write(run_query, label_query, author_data)


        # Load topics
        for _, row in topics.iterrows():
            # Split the comma-separated string into a list and remove duplicates
            topic_list = list(set([t.strip() for t in str(row.get("categories", "")).split(",")]))

            for topic in topic_list:
                topic_data = {"topic": topic}

                label_query = """
                    MERGE (t:Topic {topic: $topic})
                """

                session.execute_write(run_query, label_query, topic_data)
        
        # Load keywords
        for _, row in keywords.iterrows():
            keyword_data = {
                "name": row.get("keyword", "").strip()
            }

            label_query = """
                MERGE (k:Keyword {name: $name})
            """

            session.execute_write(run_query, label_query, keyword_data)
        #journals
        for _, row in journals.iterrows():
            journal_data = {
                "venueID": str(row.get("venueID", "")),
                "journalName": row.get("journalName", ""),
                "issn": row.get("issn", ""),
                "url": row.get("url", "")
            }

            label_query = """
                MERGE (v:Venue {venueID: $venueID})
                SET v.journalName = $journalName,
                    v.issn = $issn,
                    v.url = $url
            """

            session.execute_write(run_query, label_query, journal_data)

        for _, row in conferences.iterrows():
            conference_data = {
                "conferenceID": str(row.get("conferenceID", "")),
                "conferenceName": row.get("conferenceName", ""),
                "issn": row.get("issn", ""),
                "url": row.get("url", "")
            }

            label_query = """
                MERGE (c:Conference {conferenceID: $conferenceID})
                SET c.conferenceName = $conferenceName,
                    c.issn = $issn,
                    c.url = $url
            """
            session.execute_write(run_query, label_query, conference_data)
        # Load editions and link it to conferences
        for _, row in editions.iterrows():
            edition_data = {
                "editionID": str(row.get("editionID", "")),
                "edition": row.get("edition", ""),
                "startDate": row.get("startDate", ""),
                "endDate": row.get("endDate", "")
            }

            label_query = """
                MERGE (e:Edition {editionID: $editionID})
                SET e.edition = $edition,
                    e.startDate = $startDate,
                    e.endDate = $endDate
            """

            session.execute_write(run_query, label_query, edition_data)

        # Iterate over is_from.csv and create relationships between Edition and Conference
        for _, row in is_from.iterrows():
            relation_data = {
                "editionID": str(row.get("editionID", "")),
                "conferenceID": str(row.get("conferenceID", ""))
            }

            # Create relationship between Edition and Conference
            rel_query = """
                MATCH (e:Edition {editionID: $editionID})
                MATCH (c:Conference {conferenceID: $conferenceID})
                MERGE (e)-[:EDITION_OF]->(c)
            """

            session.execute_write(run_query, rel_query, relation_data)

        # Load volumes
        for _, row in volumes.iterrows():
            volume_data = {
                "volumeID": str(row.get("volumeID", "")),
                "year": row.get("year", ""),
                "volume": row.get("volume", "")
            }

            label_query = """
                MERGE (v:Volume {volumeID: $volumeID})
                SET v.year = $year,
                    v.volume = $volume
            """

            session.execute_write(run_query, label_query, volume_data)

        # Iterate over volume_from.csv and create relationships between Volume and Journal
        for _, row in volume_from.iterrows():
            volume_from_data = {
                "journalID": str(row.get("journalID", "")),
                "volumeID": str(row.get("volumeID", ""))
            }

            # Match the Volume and Journal nodes and create a relationship
            label_query = """
                MATCH (v:Volume {volumeID: $volumeID})
                MATCH (j:Journal {journalID: $journalID})
                MERGE (v)-[:PUBLISHED_IN]->(j)
            """

            session.execute_write(run_query, label_query, volume_from_data)

        # Iterate over journal.csv and create Journal nodes
        for _, row in journals.iterrows():
            journal_data = {
                "journalID": str(row.get("journalID", "")),
                "journalName": row.get("journalName", ""),
                "issn": row.get("issn", ""),
                "url": row.get("url", "")
            }

            label_query = """
                MERGE (j:Journal {journalID: $journalID})
                SET j.journalName = $journalName,
                    j.issn = $issn,
                    j.url = $url
            """

            session.execute_write(run_query, label_query, journal_data)

        # Relationships
        #papers to keywords
        for _, row in related_to.iterrows():
            relation_data = {
                "paperID": str(row.get("paperID", "")),
                "keyword": str(row.get("keyword", ""))
            }
            rel_query = """
                MATCH (p:Paper {id: $paperID})
                MERGE (k:Keyword {name: $keyword})
                MERGE (p)-[:RELATED_TO]->(k)
            """

            session.execute_write(run_query, rel_query, relation_data)
        
        #papers written by authors
        for _, row in written_by.iterrows():
            relation_data = {
                "paperID": str(row.get("paperID", "")),
                "authorID": str(row.get("authorID", "")),
                "is_corresponding": bool(row.get("is_corresponding", False))
            }

            # Create relationship between Paper and Author, with optional is_corresponding flag
            rel_query = """
                MATCH (p:Paper {id: $paperID})
                MATCH (a:Author {authorid: $authorID})
                MERGE (p)-[:WRITTEN_BY]->(a)
                SET p.is_corresponding = $is_corresponding
            """

            session.execute_write(run_query, rel_query, relation_data)
        
        #reviewed by authors that can be reviewers
        for _, row in reviewed_by.iterrows():
            paper_id = str(row.get("paperID", ""))
            reviewer_id = str(row.get("reviewerID", ""))
            grade = row.get("grade", "")
            review = row.get("review", "")

            # Check if the reviewer is not the main author
            main_author = written_by[(written_by['paperID'] == paper_id) & (written_by['is_corresponding'] == True)]        
            if not main_author.empty:
                main_author_id = str(main_author.iloc[0]['authorID'])
            else:
                continue
            
            # Ensure reviewer is not the main author
            if reviewer_id != main_author_id:
                rel_query = """
                    MATCH (p:Paper {id: $paperID})
                    MATCH (a:Author {authorid: $reviewerID})
                    MERGE (p)-[:REVIEWED_BY {grade: $grade, review: $review}]->(a)
                """
                
                relation_data = {
                    "paperID": paper_id,
                    "reviewerID": reviewer_id,
                    "grade": grade,
                    "review": review
                }
                
                session.execute_write(run_query, rel_query, relation_data)
        #relations cited by
        for _, row in cited_by.iterrows():
            paper_id_cited = str(row.get("paperID_cited", ""))
            paper_id_citing = str(row.get("paperID_citing", ""))
            
            rel_query = """
                MATCH (cited:Paper {id: $paperID_cited})
                MATCH (citing:Paper {id: $paperID_citing})
                MERGE (citing)-[:CITED_BY]->(cited)
            """
        
            relation_data = {
                "paperID_cited": paper_id_cited,
                "paperID_citing": paper_id_citing
            }
            session.execute_write(run_query, rel_query, relation_data)

    driver.close()

load_all()
print("Graph fully loaded into Neo4j.")