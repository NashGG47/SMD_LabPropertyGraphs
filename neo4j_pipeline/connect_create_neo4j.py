from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")

def execute_cypher_script(cypher_file_path):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            with open(cypher_file_path, "r") as file:
                cypher_queries = file.read().split(";")  # Split at semicolons
                
                for query in cypher_queries:
                    query = query.strip()
                    if query:  # Skip empty lines
                        session.run(query)
                        print(f"Executed: {query}")

cypher_file_path = "cypher_code/create_graph.cypher"
execute_cypher_script(cypher_file_path)
