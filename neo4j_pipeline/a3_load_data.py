from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import json
import os
import sys

def run_query(tx, query, parameters=None):
    tx.run(query, parameters or {})

universities = pd.read_csv("data/semantic_scholar/sc_data_csv/universities.csv")
companies = pd.read_csv("data/semantic_scholar/sc_data_csv/companies.csv")
affiliations = pd.read_csv("data/semantic_scholar/sc_data_csv/affiliated-to.csv")
reviewed_by = pd.read_csv('data/semantic_scholar/sc_data_csv/reviewed-by.csv')
def a3_load_all():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Constraints
        # Nodes
        # Load papers
        #a3 evolution of affiliations
        for _, row in universities.iterrows():
            uni_data = {
                "affiliationID": str(row.get("affiliationID", "")),
                "affiliation": row.get("affiliation", "")
            }
            label_query = """
                MERGE (u:University {uniID: $affiliationID})
                SET u.affiliationname = $affiliation
            """
            session.execute_write(run_query, label_query, uni_data)

        # Insert Companies
        for _, row in companies.iterrows():
            comp_data = {
                "affiliationID": str(row.get("affiliationID", "")),
                "affiliation": row.get("affiliation", "")
            }
            label_query = """
                MERGE (c:Company {companyID: $affiliationID})
                SET c.affiliationname = $affiliation
            """
            session.execute_write(run_query, label_query, comp_data)

        # Create Relationships (Author - University/Company)
        for _, row in affiliations.iterrows():
            aff_data = {
                "authorID": str(row.get("authorID", "")),
                "affiliationID": str(row.get("affiliationID", ""))
            }

            # University relationship
            rel_query = """
                MATCH (a:Author {authorid: $authorID})
                MATCH (u:University {uniID: $affiliationID})
                MERGE (a)-[:AFFILIATED_TO_UNI]->(u)
            """
            session.execute_write(run_query, rel_query, aff_data)

            # Company relationship
            rel2_query = """
                MATCH (a:Author {authorid: $authorID})
                MATCH (c:Company {companyID: $affiliationID})
                MERGE (a)-[:AFFILIATED_TO_COMPANY]->(c)
            """
            session.execute_write(run_query, rel2_query, aff_data)

        #a3 evolution of reviewes
        for _, row in reviewed_by.iterrows():
            relation_data = {
                "paperID": str(row.get("paperID", "")),
                "reviewerID": str(row.get("reviewerID", "")),
                "grade": int(row.get("grade", 0)),  # Ensure grade is an integer
                "review": str(row.get("review", ""))  # Ensure review is a string
            }

            reviewed_by_query = """
                MATCH (a:Author {authorid: $reviewerID})
                MATCH (p:Paper {id: $paperID})
                MERGE (a)-[r:REVIEWED_BY]->(p)
                SET r.grade = $grade, r.review = $review
            """
            session.execute_write(run_query, reviewed_by_query, relation_data)
    driver.close()

a3_load_all()
print("Graph fully loaded into Neo4j.")