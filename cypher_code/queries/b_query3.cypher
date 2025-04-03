MATCH (a:AUTHOR)-[:WRITTEN_BY]->(p:PAPER)
OPTIONAL MATCH (p)<-[:CITED_BY]-(citing:PAPER)
WITH a, p, COUNT(citing) AS citations
ORDER BY a, citations DESC
WITH a, COLLECT(citations) AS citation_list
RETURN a.name AS Author, 
       REDUCE(h = 0, i IN RANGE(0, SIZE(citation_list)-1) | 
       CASE WHEN citation_list[i] >= i+1 THEN i+1 ELSE h END) AS HIndex
