MATCH (p:PAPER)-[:CITED_BY]->(cited:PAPER), (p)-[:PUBLISHED_ON]->(conf:CONFERENCE)
WITH conf, p, COUNT(cited) AS citations
ORDER BY conf, citations DESC
WITH conf, COLLECT({paper: p, citations: citations}) AS papers
RETURN conf.name AS Conference, [p IN papers[..3] | {title: p.paper.title, citations: p.citations}] AS Top3Papers


MATCH (p:PAPER)-[:CITED_BY]->(cited:PAPER), (p)-[:PUBLISHED_ON]->(ws:WORKSHOP)
WITH ws, p, COUNT(cited) AS citations
ORDER BY ws, citations DESC
WITH ws, COLLECT({paper: p, citations: citations}) AS papers
RETURN ws.name AS Workshop, [p IN papers[..3] | {title: p.paper.title, citations: p.citations}] AS Top3Papers
