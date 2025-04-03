MATCH (a:AUTHOR)-[:WRITTEN_BY]->(p:PAPER)-[:PUBLISHED_ON]->(c:CONFERENCE),
      (c)-[:PART_OF]->(e:EDITION)
WITH a, c, COUNT(DISTINCT e) AS editions_published
WHERE editions_published >= 4
RETURN c.name AS Conference, COLLECT(a.name) AS CommunityAuthors

MATCH (a:AUTHOR)-[:WRITTEN_BY]->(p:PAPER)-[:PUBLISHED_ON]->(w:WORKSHOP),
      (w)-[:PART_OF]->(e:EDITION)
WITH a, w, COUNT(DISTINCT e) AS editions_published
WHERE editions_published >= 4
RETURN w.name AS Workshop, COLLECT(a.name) AS CommunityAuthors
