import json
from neo4j import GraphDatabase


NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"


driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))


def run_query(tx, query, parameters=None):
    tx.run(query, parameters or {})


def load_all():
    papers = []
    with open("data/semantic_scholar/examples/20240126_080619_00029_8pq62_020a9fc8-d10d-4f57-89dc-66fd71807388", "r", encoding="utf-8") as file:
        for line in file:
            papers.append(json.loads(line.strip()))
    
    return papers

# insert into Neo4j
def insert_into_neo4j(papers):
    with driver.session() as session:

        session.execute_write(run_query, "CREATE CONSTRAINT IF NOT EXISTS FOR (p:PAPER) REQUIRE p.id IS UNIQUE")
        session.execute_write(run_query, "CREATE CONSTRAINT IF NOT EXISTS FOR (a:AUTHOR) REQUIRE a.name IS UNIQUE")
        session.execute_write(run_query, "CREATE CONSTRAINT IF NOT EXISTS FOR (t:TOPIC) REQUIRE t.name IS UNIQUE")
        session.execute_write(run_query, "CREATE CONSTRAINT IF NOT EXISTS FOR (k:KEYWORDS) REQUIRE k.name IS UNIQUE")

        for paper in papers:
            session.execute_write(run_query, """
                MERGE (p:PAPER {id: $id})
                SET p.title = $title, p.year = $year, p.pages = $pages
            """, {
                "id": paper["corpusid"],
                "title": paper["title"],
                "year": paper.get("year"),
                "pages": paper.get("journal", {}).get("pages") if paper.get("journal") else None #this is needed, there are nulls so it gives out errors.
            })


            for author in paper.get("authors", []):
                session.execute_write(run_query, "MERGE (a:AUTHOR {name: $name})", {"name": author["name"]})
                session.execute_write(run_query, """
                    MATCH (a:AUTHOR {name: $name}), (p:PAPER {id: $pid})
                    MERGE (a)-[:WRITTEN_BY]->(p)
                """, {"name": author["name"], "pid": paper["corpusid"]})
            if paper.get("s2fieldsofstudy"):
                for topic in paper["s2fieldsofstudy"]:
                    session.execute_write(run_query, "MERGE (t:TOPIC {name: $name})", {"name": topic["category"]})
                    session.execute_write(run_query, """
                        MATCH (p:PAPER {id: $pid}), (t:TOPIC {name: $name})
                        MERGE (p)-[:BASED_ON]->(t)
                    """, {"pid": paper["corpusid"], "name": topic["category"]})
            if paper.get("publicationtypes"):
                for kw in paper["publicationtypes"]:
                    session.execute_write(run_query, "MERGE (k:KEYWORDS {name: $name})", {"name": kw})
                    session.execute_write(run_query, """
                        MATCH (p:PAPER {id: $pid}), (k:KEYWORDS {name: $name})
                        MERGE (p)-[:HAS_KEYWORD]->(k)
                    """, {"pid": paper["corpusid"], "name": kw})

    print("Graph successfully loaded into Neo4j.")

papers = load_all()
insert_into_neo4j(papers)


driver.close()
