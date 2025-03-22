CREATE (:Author {name: "Author Name", affiliation: "Affiliation", email: "Email"});
CREATE (:Reviewer {name: "Reviewer Name", expertise: "Expertise"});
CREATE (:Paper {title: "Paper Title", abstract: "Abstract", year: 2025});
CREATE (:Topic {name: "Topic Name"});
CREATE (:Keywords {name: "Keyword"});
CREATE (:Volume {volume_number: 1, year: 2025});
CREATE (:Journal {name: "Journal Name"});
CREATE (:Conference {name: "Conference Name"});
CREATE (:Workshop {name: "Workshop Name"});
CREATE (:Edition {edition_number: 1, year: 2025});
CREATE (:Edition {edition_number: 2, year: 2026});
MATCH (a:Author), (p:Paper)
CREATE (a)-[:WRITTEN_BY]->(p);
MATCH (p:Paper), (r:Reviewer)
CREATE (p)-[:REVIEWED_BY]->(r);
MATCH (p:Paper), (t:Topic)
CREATE (p)-[:BASED_ON]->(t);
MATCH (p:Paper), (k:Keywords)
CREATE (p)-[:BASED_ON]->(k);
MATCH (p:Paper), (v:Volume)
CREATE (p)-[:PUBLISHED_ON]->(v);
MATCH (v:Volume), (j:Journal)
CREATE (v)-[:PART_OF]->(j);
MATCH (p:Paper), (c:Conference)
CREATE (p)-[:PUBLISHED_ON]->(c);
MATCH (p:Paper), (w:Workshop)
CREATE (p)-[:PUBLISHED_ON]->(w);
MATCH (e1:Edition), (c:Conference)
CREATE (e1)-[:PART_OF]->(c);
MATCH (e2:Edition), (w:Workshop)
CREATE (e2)-[:PART_OF]->(w);
MATCH (p1:Paper), (p2:Paper)
WHERE p1.title = "Paper Title" AND p2.title = "Another Paper"
CREATE (p1)-[:CITED_BY]->(p2);
