# Analyzing Streaming Service Content in SQL

This coding project will be analyzing streaming data from the top movie and TV streaming services to answer questions regarding ratings, genres, and other content-related questions.

To do this, data will be imported in PostgreSQL for exploration and cleaning. We will then join the tables for easier analysis, use common table expressions for repeated use, and create visuals for quick insights.

## Exploring our data
Let's start by checking out the data we will be working with. We can start with the `amazon`, `hulu`, `netflix`, and `disney` tables.

```sql
SELECT *
FROM amazon
```
```sql
SELECT *
FROM hulu
```
```sql
SELECT *
FROM netflix
```
```sql
SELECT *
FROM disney
```
We can also inspect the `genres` table, which is different from the other tables.

```sql
SELECT *
FROM genres
```

## Preparing our data
### Joining the different tables
Our data appears to mostly have the same column names. So we can join the data with a series of [`UNION`](https://www.postgresql.org/docs/14/queries-union.html)'s, which will append each table to the previous one.

We use `UNION ALL` to preserve any possible duplicate rows, as we will want to count entries if they appear in multiple services.

```sql
SELECT *, 'amazon' AS service
FROM amazon
UNION ALL
SELECT *, 'hulu' AS service
FROM hulu
UNION ALL
SELECT *, 'netflix' AS service
FROM netflix
UNION ALL
SELECT *, 'disney' AS service
FROM disney
```

One problem with the above approach is that we lose out on the streaming service information. So let's repeat our previous query, but add in the required info!

```sql
SELECT *, 'amazon' AS service
FROM amazon
UNION ALL
SELECT *, 'hulu' AS service
FROM hulu
UNION ALL
SELECT *, 'netflix' AS service
FROM netflix
UNION ALL
SELECT *, 'disney' AS service
FROM disney
```

Now all of our streaming information is in one place! Next, we can add the `genres` table for further analysis.

To do this, we will need to use a [Common Table Expression](https://www.postgresql.org/docs/current/queries-with.html), or CTE.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
FROM amazon
UNION
SELECT *, 'hulu' AS service
FROM hulu
UNION
SELECT *, 'netflix' AS service
FROM netflix
UNION
SELECT *, 'disney' AS service
FROM disney
)

SELECT *
FROM service_data AS sd
LEFT JOIN genres AS g
	ON sd.title = g.film
```

### Inspecting missing data
It looks like we are missing some values in the `age` and `imdb` columns. We will also check the `rotten_tomatoes` column because we may use it later. Let's see how extensive this problem is.

To calculate the null values per column, we will use a combination of [SUM()](https://www.postgresql.org/docs/8.2/functions-aggregate.html) and [CASE WHEN](https://www.postgresql.org/docs/current/functions-conditional.html) to count the number of null values.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT
	SUM(CASE WHEN imdb IS NULL THEN 1 ELSE 0 END) AS imdb_missing,
	SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS age_missing,
	SUM(CASE WHEN rotten_tomatoes IS NULL THEN 1 ELSE 0 END) AS rt_missing
FROM all_data
```

## Analyzing our data
### Which is the most family-friendly streaming service?
Let's start by looking at the most family-friendly streaming service by the percentage of content geared towards children.

Since the genre column contains information about the primary and sub-genres, we can use [pattern matching](https://www.postgresql.org/docs/current/functions-matching.html) to find any references to "kids", "family", etc. Using the operator [ILIKE](https://www.postgresql.org/docs/18/functions-matching.html#FUNCTIONS-LIKE) will allow us to search for a pattern anywhere within a column, regardless of it's placement in the text. It is also case-insensitive so it allows us to capture a wide range of genres.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT *
FROM all_data
WHERE genre ILIKE '%kids%'
	OR genre ILIKE '%family%'
	OR genre ILIKE '%children%'
```

Great! That seems to be working. Let's adapt our query and use `CASE WHEN` to perform an aggregation and see which platform has the highest percentage of family content.

We will assign each title that matches our description a `1.0000` and those that do not with `0.0000`. We are using decimal places instead of integers to allow for calculating a percentage with some degree of accuracy, since it will most likely not be a whole number.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT
	service,
	AVG(CASE WHEN genre ILIKE '%kids%'
		OR genre ILIKE '%family%'
		OR genre ILIKE '%children%' THEN 1.0000 ELSE 0.0000 END) * 100 AS pct_family
FROM all_data
GROUP BY service
ORDER BY pct_family DESC
```

As you might expect, Disney has the highest percentage of family friendly content, almost 9x the amount available on Amazon. Now we can create a visual chart to represent this data.

### Which has the highest-rated content?
We also have access to information on the ratings of each piece of content in the `rotten_tomatoes` column. We use [`SPLIT_PART()`](https://www.postgresql.org/docs/9.1/functions-string.html) to extract the number from the column and then cast (`::`) the result as a numeric.

We will also further split the data by movie and tv shows and visualize the result in a grouped bar chart.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT
	service,
	CASE WHEN type = 1 THEN 'TV' ELSE 'Movie' END AS type,
	AVG(SPLIT_PART(rotten_tomatoes, '/', 1)::NUMERIC) AS rt_score
FROM all_data
GROUP BY service, type
ORDER BY service, type
```

### Have critics and audiences diverged over time?
For the last question, let's set the service aside and look into whether critics and audiences were more aligned on TV shows in the past. For this analysis, we will be looking at the absolute difference in ratings to view how the ratings have changed over time, but not whether they improved or decreased.

To prepare the date for the chart cell, we will need to use [`TO_DATE()`](https://www.postgresql.org/docs/current/functions-formatting.html) to convert the year into a date.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT
	date,
	AVG(ABS(imdb_score - rt_score)) AS avg_difference
FROM (
	SELECT
		TO_DATE(year::TEXT, 'YYYY') AS date,
		SPLIT_PART(rotten_tomatoes, '/', 1)::NUMERIC AS rt_score,
		SPLIT_PART(imdb, '/', 1)::NUMERIC * 10 AS imdb_score
	FROM all_data
	WHERE imdb IS NOT NULL
		AND rotten_tomatoes IS NOT NULL
		AND year >= 2000
	) AS sub
GROUP BY date
ORDER BY date
```

Now we can look into what the most divisive shows are.

```sql
WITH service_data AS (
	SELECT *, 'amazon' AS service
	FROM amazon
	UNION
	SELECT *, 'hulu' AS service
	FROM hulu
	UNION
	SELECT *, 'netflix' AS service
	FROM netflix
	UNION
	SELECT *, 'disney' AS service
	FROM disney
),

all_data AS (
	SELECT *
	FROM service_data AS sd
	LEFT JOIN genres AS g
		ON sd.title = g.film
)

SELECT
	title,
	AVG(ABS(imdb_score - rt_score)) AS avg_difference
FROM (
	SELECT
		title,
		SPLIT_PART(rotten_tomatoes, '/', 1)::NUMERIC AS rt_score,
		SPLIT_PART(imdb, '/', 1)::NUMERIC * 10 AS imdb_score
	FROM all_data
	WHERE imdb IS NOT NULL
		AND rotten_tomatoes IS NOT NULL
		AND year >= 2000
	) AS sub
GROUP BY title
ORDER BY avg_difference DESC
```

Now we have a better idea of what content is available on each service and can decide which we want to subscribe to!

### Ideas for Future Analysis

This project is only an example of how data analysis can be used for comparing streaming services. Some future ideas for analysis can include:

- Comparing ratings for film and TV across genres
- Examining the depth of content libraries for each service
- Looking at similar titles between services to analyze overlap
- Develop a layout of popular service bundles to determine which provider has the largest library for the cost
