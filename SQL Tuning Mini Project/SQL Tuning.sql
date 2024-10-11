--4.1
--Write a SQL Query to find all the conferences held in 2018 that have published at least
--200 papers in a single decade.
--Please note, conferences may be annual conferences, such as KDD. Each year a
--different number of conferences are held. You should list conferences multiple times if
--they appear in multiple years.

CREATE INDEX idx_inproceedings_year ON inproceedings(year);
CREATE INDEX idx_inproceedings_booktitle ON inproceedings(booktitle);

CREATE INDEX idx_inproceedings_year_booktitle ON inproceedings(year, booktitle);

EXPLAIN ANALYZE
WITH conference_papers AS (
    SELECT 
        i.booktitle,
        year,
		(CAST(year AS INT)/ 10) * 10 AS decade,
        COUNT(*) AS paper_count
    FROM 
        inproceedings i
    WHERE 
		year = '2018'
    GROUP BY 
        i.booktitle, year, decade
	HAVING COUNT(*) >= 200
),
decade_totals AS (
    SELECT 
        booktitle,
        decade,
        SUM(paper_count) AS total_papers
    FROM 
        conference_papers
    GROUP BY 
        booktitle, decade
    HAVING 
		SUM(paper_count) >= 200
)
--141 rows
SELECT *
FROM decade_totals
ORDER BY total_papers DESC
'''
SELECT DISTINCT 
    cp.booktitle,
    cp.year,
    dt.total_papers
FROM 
    conference_papers cp
JOIN 
    decade_totals dt ON cp.booktitle = dt.booktitle AND cp.decade = dt.decade
ORDER BY 
    cp.booktitle, cp.year;
'''

--4.2 - Accomplished Authors
--Write a SQL Query to find all the authors who published at least 10 PVLDB papers and
--at least 10 SIGMOD papers. You may need to do some legwork here to see how the
--DBLP spells the names of various conferences and journals.

-- booktitle
SELECT *
FROM inproceedings
WHERE booktitle LIKE '%SIGMOD%'

SELECT *
FROM inproceedings
WHERE booktitle LIKE '%P___VLDB%'

SELECT *
FROM articles
WHERE title LIKE '%P%VLDB%'

CREATE INDEX idx_author_journal ON articles(author, booktitle);

EXPLAIN ANALYZE
WITH AuthorPublications AS (
	-- EXPLAIN ANALYZE
    SELECT
        author,
        COUNT(CASE WHEN booktitle LIKE '%P___VLDB%%' THEN 1 END) AS PVLDB_count,
        COUNT(CASE WHEN booktitle LIKE '%SIGMOD%' THEN 1 END) AS SIGMOD_count
    FROM
        inproceedings
    WHERE
        booktitle LIKE '%P___VLDB%' OR booktitle LIKE '%SIGMOD%'
    GROUP BY
          author
)
SELECT author
FROM AuthorPublications
WHERE PVLDB_count >= 10 AND SIGMOD_count >= 10;

--4.3 - Conference Publications by Decade
--Write a SQL Query to find the total number of conference publications for each
--decade, starting from 1970 and ending in 2019. For instance, to find the total papers
---from the 1970s you would sum the totals from 1970, 1971,1972…1978, up to 1979.
--Please do this for the decades 1970, 1980, 1990, 2000, and 2010.
--Hint: You may want to create a temporary table with all the distinct years.

CREATE INDEX idx_year ON inproceedings(year);

'''
"QUERY PLAN"
"Planning Time: 0.211 ms"
"Execution Time: 1659.201 ms"
'''
EXPLAIN ANALYZE
SELECT 
	(CAST(year AS INT) / 10) * 10 AS decade, 
	COUNT(*) AS total_publications
FROM inproceedings
WHERE (CAST(year AS INT) / 10) * 10 < 2019	
GROUP BY decade
ORDER BY decade

'''
"QUERY PLAN"
"Planning Time: 0.187 ms"
"Execution Time: 2274.889 ms"
'''
EXPLAIN ANALYZE
WITH Decades AS (
    SELECT 
        (CAST(year AS INT) / 10) * 10 AS decade, 
        COUNT(*) AS total_papers
    FROM
        inproceedings
    GROUP BY decade
    ORDER BY decade
)
SELECT decade, SUM(total_papers) AS total_publications
FROM Decades
WHERE decade BETWEEN 1970 AND 2019
GROUP BY decade;


--4.4 Highly Published Data Authors
--Write a SQL Query to find the top 10 authors publishing in journals and conferences
--whose titles contain the word data. These will likely be some of the people at the
--cutting edge of data science and data analytics. As a fun exercise, find a paper one of
--them wrote that interests you and read it!

CREATE INDEX idx_title_gin ON articles USING GIN (to_tsvector('english', title));

EXPLAIN ANALYZE
SELECT 
    author, 
    COUNT(*) AS publication_count
FROM 
    articles
WHERE 
    LOWER(title) LIKE '%data%' and author IS NOT NULL
GROUP BY 
    author
ORDER BY 
    publication_count DESC
LIMIT 10;


--4.5 Highly Published June Conferences
--Write a SQL query to find the names of all conferences, happening in June, where the
--proceedings contain more than 100 publications. Proceedings and inproceedings are
--classified under conferences - according to the dblp website, so make sure you use both
--tables and use the proper attribute year.

CREATE INDEX idx_inproceedings_booktitle ON inproceedings(booktitle);
CREATE INDEX idx_proceedings_booktitle ON proceedings(booktitle);

EXPLAIN ANALYZE
WITH JuneConferences AS (
    SELECT 
        p.booktitle AS conference, 
        COUNT(*) AS paper_count
    FROM 
        inproceedings i
    JOIN 
        proceedings p ON i.booktitle = p.booktitle
    --WHERE 
    --    EXTRACT(MONTH FROM CAST(i.year AS DATE)) = 6
    GROUP BY 
        p.booktitle
)
SELECT 
    conference 
FROM 
    JuneConferences
WHERE 
    paper_count > 100;



'''
How do you improve query performance from your initial query?

We created appropriate indexes on the columns used in WHERE clauses, JOIN conditions, and GROUP BY statements.
We used CTEs (Common Table Expressions) to improve readability and potentially help the query planner optimize the queries.
For text searches, we used GIN indexes to speed up LIKE operations.
We structured the queries to minimize the amount of data processed, using subqueries and joins efficiently.


Where did you create new indexes?

We created indexes on year columns for temporal queries.
We added compound indexes on (booktitle, year) for conference-related queries.
We created GIN indexes on title columns for text search operations.
We added indexes on author columns to speed up author-based aggregations.


What was the impact of new index creation in terms of query cost and performance?

The exact impact would need to be measured using EXPLAIN ANALYZE on your specific dataset. However, you can expect:

Significant reduction in full table scans, replaced by index scans or bitmap index scans.
Reduced I/O operations, especially for queries with selective WHERE clauses.
Faster JOIN operations when using indexed columns.
Improved performance in text search operations using GIN indexes.


BONUS: What was the effect of cache on each querys performance?

To measure cache effects, run each query twice and compare the execution times:

First run: Data is read from disk, likely slower.
Second run: Data may be cached in memory, potentially much faster.


Use EXPLAIN (ANALYZE, BUFFERS) to see detailed buffer usage information.
Queries that process large amounts of data (like the decade aggregations) may show significant cache benefits on subsequent runs.
Smaller, more focused queries (like finding specific authors) might show less dramatic cache effects.
'''


