/*Task 1
Create a query for analyzing the annual sales data for the years 1999 to 2001, focusing on different sales channels and regions: 'Americas,' 'Asia,' and 'Europe.' 
The resulting report should contain the following columns:
AMOUNT_SOLD: This column should show the total sales amount for each sales channel
% BY CHANNELS: In this column, we should display the percentage of total sales for each channel (e.g. 100% - total sales for Americas in 1999, 63.64% - percentage of sales for the channel “Direct Sales”)
% PREVIOUS PERIOD: This column should display the same percentage values as in the '% BY CHANNELS' column but for the previous year
% DIFF: This column should show the difference between the '% BY CHANNELS' and '% PREVIOUS PERIOD' columns, indicating the change in sales percentage from the previous year.
The final result should be sorted in ascending order based on three criteria: first by 'country_region,' then by 'calendar_year,' and finally by 'channel_desc'*/

WITH data_source AS(	SELECT cnt.country_region,
							   tim.calendar_year,
							   chn.channel_desc,
							   sum(sls.amount_sold) AS total
						FROM sh.sales sls 
						INNER JOIN sh.channels chn  ON sls.channel_id = chn.channel_id 
						INNER JOIN sh.times tim  ON sls.time_id = tim.time_id 
						INNER JOIN sh.customers cust ON cust.cust_id = sls.cust_id
						INNER JOIN sh.countries cnt ON cnt.country_id  = cust.country_id
						WHERE tim.calendar_year BETWEEN 1998 AND 2001
						AND cnt.country_region in('Americas', 'Asia', 'Europe')
						GROUP BY cnt.country_region, tim.calendar_year, chn.channel_desc),
start_point AS (SELECT dsc.country_region,
					   dsc.calendar_year,
					   dsc.channel_desc,
					   dsc.total,
					   ROUND(dsc.total/sum(dsc.total)OVER (PARTITION BY dsc.country_region, dsc.calendar_year)*100,2) AS by_channels
					   
				FROM data_source dsc),
fin AS (SELECT stp.country_region,
	   stp.calendar_year,
	   stp.channel_desc,
	   COALESCE (stp.total, 0) || '$' AS amount_sold,
	   by_channels || '%' AS "% BY CHANNELS",
	   COALESCE (LAG (stp.by_channels) OVER (PARTITION BY stp.country_region, stp.channel_desc ORDER BY stp.calendar_year), 0)|| '%' AS "% PREVIOUS PERIOD",
	   COALESCE (stp.by_channels - LAG (stp.by_channels) OVER (PARTITION BY stp.country_region, stp.channel_desc ORDER BY stp.calendar_year),0) ||'%' AS "% DIFF" 
FROM start_point stp
ORDER BY stp.country_region, stp.calendar_year, stp.channel_desc )

SELECT *
FROM fin 
WHERE fin.calendar_year BETWEEN 1999 AND 2001
ORDER BY fin.country_region, fin.calendar_year, fin.channel_desc;

--option without LAG with window frames
WITH data_source AS(	SELECT cnt.country_region,
							   tim.calendar_year,
							   chn.channel_desc,
							   sum(sls.amount_sold) AS total
						FROM sh.sales sls 
						INNER JOIN sh.channels chn  ON sls.channel_id = chn.channel_id 
						INNER JOIN sh.times tim  ON sls.time_id = tim.time_id 
						INNER JOIN sh.customers cust ON cust.cust_id = sls.cust_id
						INNER JOIN sh.countries cnt ON cnt.country_id  = cust.country_id
						WHERE tim.calendar_year BETWEEN 1998 AND 2001
						AND cnt.country_region in('Americas', 'Asia', 'Europe')
						GROUP BY cnt.country_region, tim.calendar_year, chn.channel_desc),
						
agr_data AS (  SELECT dsc.country_region,
			   dsc.calendar_year,
			   dsc.channel_desc,
			   dsc.total,
			   ROUND(dsc.total/sum(dsc.total)OVER (PARTITION BY dsc.country_region, dsc.calendar_year)*100,2) AS by_channels,
			   ROUND( sum(dsc.total)OVER (PARTITION BY dsc.country_region, dsc.channel_desc ORDER BY dsc.calendar_year GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP )/
			          sum(dsc.total)OVER (PARTITION BY dsc.country_region ORDER BY dsc.calendar_year GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP)*100,2) AS prev_period
			   FROM data_source dsc)

SELECT agr.country_region,
	   agr.calendar_year,
	   agr.channel_desc,
	   COALESCE (agr.total, 0) || '$' AS amount_sold,
	   by_channels || '%' AS "% BY CHANNELS",
	   agr.prev_period || '%' AS "% PREVIOUS PERIOD",
	   agr.by_channels - agr.prev_period ||'%' AS "% DIFF" 
FROM agr_data agr 
WHERE agr.calendar_year BETWEEN 1999 AND 2001
ORDER BY agr.country_region, agr.calendar_year, agr.channel_desc;

/*Task 2
You need to create a query that meets the following requirements:
Generate a sales report for the 49th, 50th, and 51st weeks of 1999.
Include a column named CUM_SUM to display the amounts accumulated during each week.
Include a column named CENTERED_3_DAY_AVG to show the average sales for the previous, current, and following days using a centered moving average.
For Monday, calculate the average sales based on the weekend sales (Saturday and Sunday) as well as Monday and Tuesday.
For Friday, calculate the average sales on Thursday, Friday, and the weekend.


Ensure that your calculations are accurate for the beginning of week 49 and the end of week 51.
*/
WITH source_data AS (SELECT tim.calendar_week_number,
						    tim.time_id AS time_id,
						    tim.day_name,	   
						    COALESCE (sum(sls.amount_sold),0) AS sales
					FROM sh.times tim
					LEFT JOIN  sh.sales sls ON sls.time_id = tim.time_id 
					WHERE tim.calendar_year = 1999
					AND tim.calendar_week_number BETWEEN 48 AND 52
					GROUP BY tim.calendar_week_number, tim.time_id::date, tim.day_name),				
					
calc_data AS (SELECT *,
						ROUND(SUM(src.sales)OVER (PARTITION BY src.calendar_week_number ORDER BY src.time_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) AS cum_sum,
						ROUND(	CASE 
								WHEN src.day_name = 'Monday' THEN AVG(src.sales)OVER (ORDER BY src.time_id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)
								WHEN src.day_name = 'Friday' THEN AVG(src.sales)OVER (ORDER BY src.time_id ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
								ELSE AVG(src.sales)OVER (ORDER BY src.time_id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
							END,2) AS centered_3_day_avg
						
			  FROM source_data src)
SELECT cld.calendar_week_number,
	   cld.time_id::date,
	   cld.day_name,
	   cld.sales,
	   cld.cum_sum,
	   cld.centered_3_day_avg
FROM calc_data cld
WHERE cld.calendar_week_number BETWEEN 48 AND 51;

/*Please provide 3 instances of utilizing window functions that include a frame clause, using RANGE, ROWS, and GROUPS modes. 
Additionally, explain the reason for choosing a specific frame type for each example. 
This can be presented as a single query or as three distinct queries.*/

/*In this example, I wanted to demonstrate the use of a cumulative sum for product categories.
In the version with ROWS, we sum the revenue by subcategories row by row.
In the case of RANGE and GROUPS, when there are two rows with the same subcategory name, the total is calculated and displayed for the entire subcategory.
*/
WITH source_data AS (SELECT prd.prod_category,
					        prd.prod_subcategory,
					        prd.prod_name,
					        sum(sls.amount_sold) AS total
					FROM sh.sales sls
					INNER JOIN sh.products prd ON sls.prod_id = prd.prod_id
					INNER JOIN sh.times tim ON  sls.time_id = tim.time_id
					WHERE tim.calendar_year =2000
					GROUP BY prd.prod_category, prd.prod_subcategory, prd.prod_name
					ORDER BY prd.prod_category, prd.prod_subcategory)
SELECT *,
	   SUM(total)OVER (PARTITION BY src.prod_category ORDER BY prod_subcategory ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_rows, --
	   SUM(total)OVER (PARTITION BY src.prod_category ORDER BY prod_subcategory GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_groups,
	   SUM(total)OVER (PARTITION BY src.prod_category ORDER BY prod_subcategory RANGE  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_range
FROM source_data src;


/*In this example, I also used a cumulative sum by dates.
When using ROWS, the values are summed row by row.
With GROUPS, identical dates form a group: the values within the group are summed, and then the value of the previous group is added.
With RANGE, identical dates also form a group and are summed, but then the value for the previous three days is added, regardless of how many identical rows there were.

In this example, you need to scroll the results a bit to clearly see the difference.*/

WITH src_data AS (	SELECT tim.time_id::date AS time_id,
						   COALESCE (sls.amount_sold, 0) AS amount_sold
					FROM sh.times tim
					LEFT JOIN sh.sales sls ON  sls.time_id = tim.time_id
					INNER JOIN sh.products prd ON sls.prod_id = prd.prod_id
					WHERE tim.calendar_year =1999 AND tim.calendar_month_number = 1  AND prd.prod_category ='Electronics'
					ORDER BY tim.time_id),
					
days_month AS (	 SELECT tim.time_id::date AS d_time
				      FROM sh.times tim
				      WHERE tim.calendar_year = 1999
				      AND tim.calendar_month_number = 1	
				      ORDER BY d_time),
data_read AS(SELECT dmn.d_time,
					COALESCE (amount_sold,0) AS amount_sold
			 FROM days_month dmn
			 LEFT JOIN src_data src ON dmn.d_time= src.time_id)
SELECT *,
	   sum(dr.amount_sold)over(ORDER BY dr.d_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum_row,
	   sum(dr.amount_sold)over(ORDER BY dr.d_time GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum_group,
	   sum(dr.amount_sold)over(ORDER BY dr.d_time RANGE BETWEEN INTERVAL '3 days' PRECEDING AND CURRENT ROW) AS cum_sum_range_3_days
FROM data_read dr;       