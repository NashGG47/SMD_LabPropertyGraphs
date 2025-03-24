CREATE (:AUTHOR {name: "Author Name", affiliation: "Affiliation", email: "Email"});
CREATE (:TOPIC {name: "Topic Name"});
CREATE (:KEYWORDS {name: "Keyword Name"});
CREATE (:PAPER {id: "PaperID", title: "Title", abstract: "Abstract", doi: "DOI", pages: "Pages"});
CREATE (:VOLUME {volume: "Volume", year: "Year"});
CREATE (:JOURNAL {publisher: "Publisher", name: "Journal Name"});
CREATE (:CONFERENCE {id: "ConfID", name: "Conference Name"});
CREATE (:WORKSHOP {id: "WorkshopID", name: "Workshop Name"});
CREATE (:EDITION {id: "EditionID", city: "City", year: "Year", venue: "Venue"});
MATCH (a:AUTHOR), (p:PAPER)
CREATE (a)-[:WRITTEN_BY]->(p);
MATCH (a:AUTHOR), (p:PAPER)
CREATE (a)-[:CORRESPONDED_BY]->(p);
MATCH (p:PAPER)<-[:WRITTEN_BY]-(a:AUTHOR), (r:AUTHOR)
WHERE a <> r
CREATE (r)-[:REVIEWED_BY]->(p);
MATCH (p:PAPER), (r:AUTHOR)
CREATE (r)-[:REVIEWED_BY]->(p);
MATCH (a:AUTHOR), (t:TOPIC)
CREATE (a)-[:EXPERT_IN]->(t);
MATCH (t:TOPIC), (p:PAPER)
CREATE (t)-[:BASED_ON]->(p);
MATCH (p1:PAPER), (p2:PAPER)
CREATE (p1)-[:CITED_BY]->(p2);
MATCH (p:PAPER), (k:KEYWORDS)
CREATE (p)-[:BASED_ON]->(k);
MATCH (p:PAPER), (v:VOLUME)
CREATE (p)-[:PUBLISHED_ON]->(v);
MATCH (v:VOLUME), (j:JOURNAL)
CREATE (v)-[:PART_OF]->(j);
MATCH (p:PAPER), (c:CONFERENCE)
CREATE (p)-[:PUBLISHED_ON]->(c);
MATCH (c:CONFERENCE), (e:EDITION)
CREATE (c)-[:PART_OF]->(e);
MATCH (p:PAPER), (w:WORKSHOP)
CREATE (p)-[:PUBLISHED_ON]->(w);
MATCH (w:WORKSHOP), (e:EDITION)
CREATE (w)-[:PART_OF]->(e);
MATCH (c:CONFERENCE)-[:PART_OF]->(e:EDITION), (w:WORKSHOP)-[:PART_OF]->(e)
WHERE c IS NOT NULL AND w IS NOT NULL
DELETE (w)-[:PART_OF]->(e); 
MATCH (e:EDITION), (c:CONFERENCE), (w:WORKSHOP)
WHERE (c)-[:PART_OF]->(e) AND NOT (w)-[:PART_OF]->(e)
CREATE (w)-[:PART_OF]->(e);
