papers = pd.read_csv("data/semantic_scholar/sc_data_csv/papers-processed.csv")
authors = pd.read_csv("data/semantic_scholar/sc_data_csv/authors-sample.csv")


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
        delete_query = """
            MATCH (n)
            DETACH DELETE n
        """
        session.execute_write(run_query, delete_query)
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

    driver.close()

load_all()
print("Graph fully loaded into Neo4j.")