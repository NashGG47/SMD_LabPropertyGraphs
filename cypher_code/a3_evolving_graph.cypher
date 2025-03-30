CREATE (f:AFFILIATION {name: 'affiliation_name'})
WITH f
MATCH (a:Author)
WHERE a.name = 'author_name'
CREATE (a)-[:AFFILIATED_TO]->(f);


MATCH (a:Author)-[r:REVIEWED_BY]->(p:Paper)
SET r.content = 'review_content', r.suggested_decision = 'suggested_decision';

MATCH (c:Conference)
SET c.number_of_reviewers = 3;
MATCH (w:Workshop)
SET w.number_of_reviewers = 3;
MATCH (j:Journal)
SET j.number_of_reviewers = 3;
