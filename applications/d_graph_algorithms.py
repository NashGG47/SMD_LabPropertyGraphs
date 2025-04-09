from neo4j import GraphDatabase


USER = "neo4j"
PASSWORD = "password"


driver = GraphDatabase.driver("bolt://localhost:7687", auth=(USER, PASSWORD))

def run_pagerank():
    with driver.session() as session:

        graph_exists = session.run("CALL gds.graph.exists('papers-graph')").single()[1]
        if graph_exists:
            session.run("CALL gds.graph.drop('papers-graph') YIELD graphName")


        session.run("""
            CALL gds.graph.project(
                'papers-graph',
                'Paper',
                {
                    CITED_BY: {},
                    RELATED_TO: {}
                }
            )
        """)

        result = session.run("""
            CALL gds.pageRank.stream('papers-graph')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS paperId, score
            ORDER BY score DESC
            LIMIT 10
        """)


        print("PageRank algorithm results:")
        for record in result:
            print(f"Paper ID: {record['paperId']}, Score: {record['score']}")
        session.run("CALL gds.graph.drop('papers-graph')")

def run_louvain():
    with driver.session() as session:

        graph_exists = session.run("CALL gds.graph.exists('papers-graph')").single()[1]
        if graph_exists:
            session.run("CALL gds.graph.drop('papers-graph') YIELD graphName")


        session.run("""
            CALL gds.graph.project(
                'papers-graph',
                'Paper',
                {
                    CITED_BY: {},
                    RELATED_TO: {}
                }
            )
        """)
        result = session.run("""
            CALL gds.louvain.stream('papers-graph')
            YIELD nodeId, communityId
            RETURN gds.util.asNode(nodeId).id AS paperId, communityId
            ORDER BY communityId ASC
            LIMIT 10
        """)
        print("Louvain algorithm results:")
        for record in result:
            print(f"Paper ID: {record['paperId']}, Community ID: {record['communityId']}")

        session.run("CALL gds.graph.drop('papers-graph')")
