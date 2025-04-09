"""
C_Recommender.py

This script implements the reviewer recommender for Part C of the project.

It performs the following steps using Cypher queries:
1. Defines the database research community by linking it to domain-specific keywords.
2. Identifies venues (conferences/journals) with at least 90% of papers related to the community.
3. Selects the top 100 most cited papers from those venues.
4. Tags authors of these papers as potential reviewers and highlights gurus with multiple top papers.
"""

from neo4j import GraphDatabase

# Replace with your actual Neo4j credentials if needed
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "password"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def recommender():
    try:
        with driver.session() as session:
            print("1. Executing recommender...")

            # Step 0 - Clean up previous state
            session.run('''
            DROP CONSTRAINT communityNameConstraint IF EXISTS;
            ''')

            session.run('''
            MATCH (c:Community)-[r:DEFINED_BY]->()
            DETACH DELETE c
            ''')

            # Step 1 - Define the Research Community
            session.run('''
            CREATE CONSTRAINT communityNameConstraint FOR (c:Community) REQUIRE c.name IS UNIQUE;
            ''')

            session.run('''
            CREATE (community:Community {name: 'database'})
            WITH community
            MATCH (k:Keyword)
            WHERE k.name IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
            MERGE (community)-[:DEFINED_BY]->(k)
            ''')

            # Step 2 - Identify Relevant Venues (≥90% match)
            session.run('''
            MATCH (c:Community {name: 'database'})-[:DEFINED_BY]->(k:Keyword)
            MATCH (p:Paper)-[:RELATED_TO]->(k)
            MATCH (p)-[:PUBLISHED_ON|PUBLISHED_ON_C]->(v)
            WITH v, COUNT(DISTINCT p) AS papersWithKW, c
            MATCH (allPapers:Paper)-[:PUBLISHED_ON|PUBLISHED_ON_C]->(v)
            WITH v, papersWithKW, COUNT(DISTINCT allPapers) AS totalPapers, c
            WHERE totalPapers > 0 AND (toFloat(papersWithKW) / totalPapers) >= 0.9
            MERGE (v)-[:IN_COMMUNITY]->(c)
            ''')

            # Step 3 - Identify Top-100 Papers
            session.run('''
            MATCH (v)-[:IN_COMMUNITY]->(c:Community {name: 'database'})
            MATCH (p:Paper)-[:PUBLISHED_ON|PUBLISHED_ON_C]->(v)
            MATCH (p)<-[:CITED_BY]-(citing:Paper)
            WITH p, COUNT(citing) AS numCitations
            ORDER BY numCitations DESC
            LIMIT 100
            SET p.is_database_com_top = true
            ''')

            # Step 4.1 - Tag Potential Reviewers
            session.run('''
            MATCH (p:Paper {is_database_com_top: true})-[:WRITTEN_BY]->(a:Author)
            SET a.potential_database_com_rev = true
            ''')

            # Step 4.2 - Tag Gurus
            session.run('''
            MATCH (p:Paper {is_database_com_top: true})-[:WRITTEN_BY]->(a:Author)
            WITH a, COUNT(p) AS topCount
            WHERE topCount >= 2
            SET a.database_com_guru = true
            ''')

            print("2. Results:")
            print("-" * 70)

            reviewers = session.run('''
            MATCH (a:Author {potential_database_com_rev: true})
            RETURN DISTINCT a.name AS Author
            ''')
            print("Potential Reviewers:")
            for record in reviewers:
                print(f"- {record['Author']}")

            print("-" * 70)
            gurus = session.run('''
            MATCH (a:Author {database_com_guru: true})
            RETURN DISTINCT a.name AS Author
            ''')
            print("Database Community Gurus:")
            for record in gurus:
                print(f"- {record['Author']}")

    except Exception as e:
        print("An error occurred while executing the recommender:")
        print(e)

    finally:
        driver.close()
        print("\nSession closed.")

# if __name__ == "__main__":
#     recommender()
