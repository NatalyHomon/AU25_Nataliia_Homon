/*Task 1
Create a query to produce a sales report highlighting the top customers with the highest sales across different sales channels.
 This report should list the top 5 customers for each channel. Additionally, calculate a key performance indicator (KPI) called 'sales_percentage,'
  which represents the percentage of a customer's sales relative to the total sales within their respective channel.
Please format the columns as follows:
Display the total sales amount with two decimal places
Display the sales percentage with four decimal places and include the percent sign (%) at the end
Display the result for each channel in descending order of sales

*/
 
SELECT ttl.channel_desc,
	   cust.cust_last_name,
	   cust.cust_first_name,
	   ttl.amount_sold,
	   ttl.sales_percentage
	  
FROM(	  
		SELECT  chn.channel_desc,
				sls.cust_id,
			    ROUND(sum(sls.amount_sold), 2) AS amount_sold,
			    Round(sum(sls.amount_sold) / sum(sum(sls.amount_sold))OVER (PARTITION BY chn.channel_desc) *100, 4) || '%' AS sales_percentage,
			    rank() OVER (PARTITION BY chn.channel_desc ORDER BY sum(sls.amount_sold) DESC) rnk  --If the task literally requires "top 5 customers" and expects exactly 5 rows per channel, I can replace RANK() with ROW_NUMBER().
		FROM sh.sales sls
		INNER JOIN sh.channels chn ON chn.channel_id  = sls.channel_id 
		GROUP BY sls.cust_id, chn.channel_desc
		
		) AS ttl
INNER JOIN sh.customers cust ON ttl.cust_id = cust.cust_id
WHERE rnk BETWEEN 1 AND 5
ORDER BY ttl.channel_desc, ttl.amount_sold DESC;

/*Task 2
Create a query to retrieve data for a report that displays the total sales for all products in the Photo category in the Asian region for the year 2000. Calculate the overall report total and name it 'YEAR_SUM'
Display the sales amount with two decimal places
Display the result in descending order of 'YEAR_SUM'
For this report, consider exploring the use of the crosstab function. Additional details and guidance can be found at this link
*/
--simple option, prefer it
SELECT prd.prod_name,
	   ROUND(SUM(CASE WHEN EXTRACT(QUARTER FROM sls.time_id) = 1 THEN sls.amount_sold ELSE 0 END), 2) AS q1,
       ROUND(SUM(CASE WHEN EXTRACT(QUARTER FROM sls.time_id) = 2 THEN sls.amount_sold ELSE 0 END), 2) AS q2,
       ROUND(SUM(CASE WHEN EXTRACT(QUARTER FROM sls.time_id) = 3 THEN sls.amount_sold ELSE 0 END), 2) AS q3,
       ROUND(SUM(CASE WHEN EXTRACT(QUARTER FROM sls.time_id) = 4 THEN sls.amount_sold ELSE 0 END), 2) AS q4,
	   SUM(amount_sold)  AS year_sum
FROM sh.sales sls
INNER JOIN sh.products prd ON sls.prod_id = prd.prod_id
INNER JOIN sh.customers cust ON cust.cust_id = sls.cust_id 
INNER JOIN sh.countries cntr ON cust.country_id  = cntr.country_id 
WHERE EXTRACT (YEAR FROM sls.time_id) = 2000
AND prd.prod_category = 'Photo'
AND cntr.country_region = 'Asia'
GROUP BY prd.prod_name
ORDER BY year_sum DESC;

--option with crosstab function, believewe don't need it in this case
CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT 
    prod_name,
    COALESCE(q1,0) AS q1,
    COALESCE(q2,0) AS q2,
    COALESCE(q3,0) AS q3,
    COALESCE(q4,0) AS q4,
    ROUND(
        COALESCE(q1,0)+COALESCE(q2,0)+COALESCE(q3,0)+COALESCE(q4,0)
    , 2) AS year_sum
FROM (
    SELECT *
    FROM sh.crosstab(
        -- SQL з row_name, category, value
        $$ 
        SELECT 
            prd.prod_name, 
            EXTRACT(QUARTER FROM sls.time_id)::int AS quarter_num,
            SUM(sls.amount_sold) AS quarter_sales
        FROM sh.sales sls
        INNER JOIN sh.products prd ON sls.prod_id = prd.prod_id
        INNER JOIN sh.customers cust ON cust.cust_id = sls.cust_id
        INNER JOIN sh.countries cntr ON cust.country_id = cntr.country_id
        WHERE EXTRACT(YEAR FROM sls.time_id) = 2000
          AND prd.prod_category = 'Photo'
          AND cntr.country_region = 'Asia'
        GROUP BY prd.prod_name, EXTRACT(QUARTER FROM sls.time_id)
        ORDER BY prd.prod_name, EXTRACT(QUARTER FROM sls.time_id)
        $$::text,
        -- SQL з переліком категорій (квартали)
        $$ SELECT generate_series(1,4) $$::text
    ) AS ct(
        prod_name TEXT,
        q1 NUMERIC,
        q2 NUMERIC,
        q3 NUMERIC,
        q4 NUMERIC
    )
) t
ORDER BY year_sum DESC;


/*Task 3
Create a query to generate a sales report for customers ranked in the top 300 based on total sales in the years 1998, 1999, and 2001. The report should be categorized based on sales channels, and separate calculations should be performed for each channel.
Retrieve customers who ranked among the top 300 in sales for the years 1998, 1999, and 2001
Categorize the customers based on their sales channels
Perform separate calculations for each sales channel
Include in the report only purchases made on the channel specified
Format the column so that total sales are displayed with two decimal places

*/
--the first option with joins 
WITH source_data AS(
					SELECT 		   cust.cust_id,
								   chn.channel_desc,
								   SUM(sls.amount_sold) AS total_chan,
								   EXTRACT (YEAR FROM sls.time_id) AS year_sales,
								   RANK() OVER (PARTITION BY chn.channel_desc, EXTRACT (YEAR FROM sls.time_id) ORDER BY SUM(sls.amount_sold) desc) AS top_300			   
							FROM sh.sales sls
							INNER JOIN sh.customers cust ON cust.cust_id = sls.cust_id
							INNER JOIN sh.channels chn ON sls.channel_id  = chn.channel_id 
							WHERE EXTRACT (YEAR FROM sls.time_id) IN (1998, 1999, 2001)
							GROUP BY cust.cust_id, chn.channel_desc, EXTRACT (YEAR FROM sls.time_id)
),data_1998 as(
				SELECT srs.cust_id,
					   srs.channel_desc,
					   srs.total_chan,
					   srs.year_sales,
					   srs.top_300
				FROM source_data AS srs
				WHERE top_300 <=300 
				AND srs.year_sales=1998
)
SELECT d98.channel_desc,
	   d98.cust_id,
	   cust.cust_last_name,
	   cust.cust_first_name,
	   ROUND(d98.total_chan + d99.total_chan + d21.total_chan, 2) AS amount_sold	   
FROM data_1998 AS d98
LEFT JOIN source_data d99 ON d98.cust_id = d99.cust_id AND d99.year_sales = 1999 AND d99.top_300<=300 AND d98.channel_desc =d99.channel_desc
LEFT JOIN source_data d21 ON d98.cust_id = d21.cust_id AND d21.year_sales = 2001 AND d21.top_300<=300 AND d98.channel_desc =d21.channel_desc
INNER JOIN sh.customers cust ON d98.cust_id = cust.cust_id
WHERE d99.cust_id IS NOT NULL 
	AND d21.cust_id IS NOT NULL
	ORDER BY amount_sold DESC;
	
--second option	(better one -was looking for a simpler solution without joins)
WITH top300 AS (
			  SELECT
			      sls.cust_id,
			      chn.channel_desc,
			      EXTRACT(YEAR FROM sls.time_id) AS year_sales,
			      SUM(sls.amount_sold) AS total_chan,
			      RANK() OVER (PARTITION BY chn.channel_desc, EXTRACT (YEAR FROM sls.time_id) ORDER BY SUM(sls.amount_sold) desc) AS top_300
			  FROM sh.sales sls
			  JOIN sh.channels chn ON chn.channel_id = sls.channel_id
			  WHERE EXTRACT(YEAR FROM sls.time_id) IN (1998, 1999, 2001)
			  GROUP BY sls.cust_id, chn.channel_desc, EXTRACT(YEAR FROM sls.time_id)
)
SELECT
    top.channel_desc,
    top.cust_id,
    cust.cust_last_name,
    cust.cust_first_name,
    ROUND(
      COALESCE(SUM(top.total_chan) FILTER (WHERE top.year_sales = 1998), 0) +   ---instead case when
      COALESCE(SUM(top.total_chan) FILTER (WHERE top.year_sales = 1999), 0) +
      COALESCE(SUM(top.total_chan) FILTER (WHERE top.year_sales = 2001), 0),
      2
    ) AS amount_sold
FROM top300 top
JOIN sh.customers cust ON cust.cust_id = top.cust_id
WHERE top.top_300 <= 300
GROUP BY top.channel_desc, top.cust_id, cust.cust_last_name, cust.cust_first_name
HAVING COUNT(DISTINCT top.year_sales) = 3
ORDER BY amount_sold DESC;


/*Task 4
Create a query to generate a sales report for January 2000, February 2000, and March 2000 specifically for the Europe and Americas regions.
Display the result by months and by product category in alphabetical order.
*/

/*Due to aggregation by regions, I have duplicate rows. 
I used distinct for visualization, but maybe there is a better approach to solving this problem with window functions.
Below is another option without using window functions, only grouping and aggregation.*/

SELECT 	distinct TO_CHAR(sls.time_id, 'YYYY-MM') AS calendar_month_desc,
		prd.prod_category,
	    ROUND(SUM(CASE WHEN cnt.country_region='Americas' THEN SUM(sls.amount_sold) END) OVER (PARTITION BY TO_CHAR(sls.time_id, 'YYYY-MM') ORDER BY prd.prod_category),2) AS "Americas  SALES",
   	    ROUND(SUM(CASE WHEN cnt.country_region='Europe' THEN SUM(sls.amount_sold) END) OVER (PARTITION BY TO_CHAR(sls.time_id, 'YYYY-MM') ORDER BY prd.prod_category),2) AS "Europe SALES"
FROM sh.sales sls
INNER JOIN sh.products prd ON sls.prod_id  = prd.prod_id 	
JOIN sh.customers cust ON cust.cust_id = sls.cust_id
JOIN sh.countries cnt  ON cnt.country_id = cust.country_id
WHERE EXTRACT(YEAR FROM sls.time_id) =2000 
AND EXTRACT(MONTH FROM sls.time_id) IN (1, 2, 3)
AND cnt.country_region IN ('Europe','Americas')
GROUP BY prd.prod_category, TO_CHAR(sls.time_id, 'YYYY-MM'), cnt.country_region
ORDER BY prd.prod_category;
	


--without window functions
SELECT
    TO_CHAR(DATE_TRUNC('month', sls.time_id), 'YYYY-MM') AS year_month,
    prd.prod_category,    
    ROUND(SUM(sls.amount_sold) FILTER (WHERE cnt.country_region = 'Americas'), 2) AS "Americas  SALES",
    ROUND(SUM(sls.amount_sold) FILTER (WHERE cnt.country_region = 'Europe'), 2)   AS "Europe SALES"
FROM sh.sales sls
JOIN sh.products prd   ON prd.prod_id = sls.prod_id
JOIN sh.customers cust ON cust.cust_id = sls.cust_id
JOIN sh.countries cnt  ON cnt.country_id = cust.country_id
WHERE sls.time_id >= DATE '2000-01-01'
  AND sls.time_id <  DATE '2000-04-01'
  AND cnt.country_region IN ('Europe', 'Americas')
GROUP BY
    DATE_TRUNC('month', sls.time_id),
    prd.prod_category
ORDER BY
    year_month,
    prd.prod_category;
