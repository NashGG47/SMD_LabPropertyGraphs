"""
b_queries.py

This script executes the four required queries for Part B.

1. Top 3 most cited papers for each Journal (using the PUBLISHED_IN_VENUE relationship).
2. For each Journal, find its community, defined as authors that have published at least 4 papers in that Journal.
3. Compute the impact factor for each Journal as the average number of citations per paper.
4. Compute the h‑index for each Author.
"""

from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "password"  

# --- Query Definitions ---
def query_top3_cited_papers_by_journal(tx):
    """
    For each Journal (via the PUBLISHED_ON relationship), find the top 3 most cited papers.
    """
    cypher = """
    MATCH (j:Journal)<-[:PUBLISHED_ON]-(p:Paper)
    OPTIONAL MATCH (p)<-[:CITED_BY]-(cp:Paper)
    WITH j, p, count(cp) AS citationCount
    ORDER BY j.journalName, citationCount DESC
    WITH j, collect({paper: p, citations: citationCount}) AS papers
    RETURN j.journalName AS Journal, papers[0..3] AS Top3Papers
    """
    return list(tx.run(cypher))

def query_journal_communities(tx):
    """
    For each Journal, find its community defined as authors that have published at least 4 papers in that Journal.
    """
    cypher = """
    MATCH (a:Author)<-[:WRITTEN_BY]-(p:Paper)-[:PUBLISHED_ON]->(j:Journal)
    WITH j, a, count(p) AS paperCount
    WHERE paperCount >= 4
    RETURN j.journalName AS Journal, collect(DISTINCT a.name) AS Community
    """
    return list(tx.run(cypher))

def query_journal_impact_factor(tx):
    """
    Computes the impact factor for each Journal as the average number of citations per paper.
    """
    cypher = """
    MATCH (j:Journal)<-[:PUBLISHED_ON]-(p:Paper)
    OPTIONAL MATCH (p)<-[:CITED_BY]-(cp:Paper)
    WITH j, p, count(cp) AS citationCount
    WITH j, avg(citationCount) AS ImpactFactor
    RETURN j.journalName AS Journal, ImpactFactor
    """
    return list(tx.run(cypher))

def query_author_hindex(tx):
    """
    Computes the h-index for each Author based on the citations of their papers.
    We use a subquery (CALL { ... }) to sort the individual paper citation counts in descending order,
    and then compute the h-index as the maximum integer h such that the author has at least h papers with h or more citations.
    """
    cypher = """
    MATCH (a:Author)
    CALL {
       WITH a
       MATCH (a)<-[:WRITTEN_BY]-(p:Paper)
       OPTIONAL MATCH (p)<-[:CITED_BY]-(cp:Paper)
       WITH p, count(cp) as citations
       ORDER BY citations DESC
       RETURN collect(citations) as sortedCitations
    }
    UNWIND range(0, size(sortedCitations)-1) as idx
    WITH a, sortedCitations, idx, sortedCitations[idx] as currentCitation
    WHERE currentCitation >= idx + 1
    RETURN a.name as Author, max(idx + 1) as hIndex
    """
    return list(tx.run(cypher))

# --- Main Execution ---
def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        with driver.session() as session:
            print("Query 1: Top 3 most cited papers for each Journal")
            top3_results = session.execute_read(query_top3_cited_papers_by_journal)
            for record in top3_results:
                journal = record["Journal"]
                papers = record["Top3Papers"]
                print(f"\nJournal: {journal}")
                for idx, p in enumerate(papers, start=1):
                    paper_title = p["paper"].get("title", "No title")
                    citations = p["citations"]
                    print(f"  {idx}. {paper_title} (Citations: {citations})")

            print("\nQuery 2: Journal communities (authors with >= 4 papers in the Journal)")
            community_results = session.execute_read(query_journal_communities)
            if community_results:
                for record in community_results:
                    print(f"Journal: {record['Journal']}")
                    authors = record["Community"]
                    print("  Community Authors:", ", ".join(authors))
            else:
                print("No journal communities found.")

            print("\nQuery 3: Journal Impact Factor (average citations per paper)")
            impact_results = session.execute_read(query_journal_impact_factor)
            for record in impact_results:
                journal = record["Journal"]
                impact_factor = record["ImpactFactor"]
                print(f"Journal: {journal} -> Impact Factor: {impact_factor:.2f}")

            print("\nQuery 4: Authors' h-index")
            hindex_results = session.execute_read(query_author_hindex)
            for record in hindex_results:
                print(f"Author: {record['Author']} -> h-index: {record['hIndex']}")
    finally:
        driver.close()

# if __name__ == "__main__":
#     main()
