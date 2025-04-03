MATCH (j:JOURNAL)<-[:PART_OF]-(v:VOLUME)<-[:PUBLISHED_ON]-(p:PAPER)
WHERE v.year >= (date().year - 2)
OPTIONAL MATCH (p)<-[:CITED_BY]-(cited:PAPER)
WITH j, COUNT(DISTINCT cited) AS total_citations, COUNT(DISTINCT p) AS total_papers
WHERE total_papers > 0
RETURN j.name AS Journal, total_citations / total_papers AS ImpactFactor
