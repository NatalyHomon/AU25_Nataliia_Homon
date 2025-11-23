/*Task 1. Create a view. Create a viefilmw called 'sales_revenue_by_category_qtr' that shows the film category and total sales revenue for the current quarter and year. The view should only display categories with at least one sale in the current quarter. 
Note: make it dynamic - when the next quarter begins, it automatically considers that as the current quarter                                                                                                                             
*/                                                                                                                                                                                                                                       
--max(payment_date =2017-01-24 20:21:56.996 +0200  ) min(payment_date =2017-01-24 20:21:56.996 +0200 ) 
--in this case I used an existing date to check the correctness of the code execution. To make the code universal and calculate the income for the quarter at the current time, you need to replace the date with current_date                                                                                                                                                                                                                                     
CREATE OR REPLACE VIEW sales_revenue_by_category_qtr AS (   

--connect category name with film table
WITH film_category_name as(
							SELECT cat.name AS cat_name, cat.category_id, flm.film_id
							FROM public.film_category flmc
							INNER JOIN public.category cat ON flmc.category_id = cat.category_id
							INNER JOIN public.film flm ON flm.film_id = flmc.film_id
							
),--find total revenue by each film in the quarter(if use current date=> table is empty as we don't have payments in this 2025 4th quarter, so to show that it works I used the existing date from table payments)
film_total_revenue AS (
						SELECT inv.film_id, sum(pay.amount) film_revenue, date_trunc('quarter', '2017-05-01'::date) AS current_quarter_of_year
						FROM public.inventory inv
						LEFT JOIN public.rental rnt ON inv.inventory_id = rnt.inventory_id 
						LEFT JOIN public.payment pay ON pay.rental_id = rnt.rental_id 
						WHERE payment_date >= date_trunc('quarter', '2017-05-01'::date)    --reusable =>change '2017-05-01'::date to current_date
  								AND payment_date <  date_trunc('quarter', '2017-05-01'::date)+ interval '3 month'  --reusable =>change '2017-05-01'::date to current_date
						GROUP BY inv.film_id
)

SELECT flmc.cat_name AS film_category,
	   sum(ftr.film_revenue) AS revenue_for_quarter,
	   EXTRACT(YEAR FROM ftr.current_quarter_of_year) AS current_year,
	   EXTRACT(QUARTER FROM ftr.current_quarter_of_year) as current_quarter
FROM film_category_name flmc
INNER JOIN film_total_revenue ftr ON ftr.film_id = flmc.film_id --inner join should only display categories with at least one sale in the current quarter
GROUP BY flmc.cat_name, ftr.current_quarter_of_year

);

--check
SELECT *
FROM sales_revenue_by_category_qtr;

/*Task 2. Create a query language functions. Create a query language function called 'get_sales_revenue_by_category_qtr' that accepts one parameter representing the
 *  current quarter and year and returns the same result as the 'sales_revenue_by_category_qtr' view.*/

CREATE OR REPLACE FUNCTION get_sales_revenue_by_category_qtr (IN current_date_of_the_year date DEFAULT NULL) 
RETURNS TABLE (film_category		 text,
			   revenue_for_quarter	 numeric,
			   current_year 		 int,
			   current_quarter		 int)
LANGUAGE SQL 
AS 
$$--connect category name with film table
WITH film_category_name as(
							SELECT cat.name AS cat_name, cat.category_id, flm.film_id
							FROM public.film_category flmc
							INNER JOIN public.category cat ON flmc.category_id = cat.category_id
							INNER JOIN public.film flm ON flm.film_id = flmc.film_id
							
),--find total revenue by each film in the quarter
film_total_revenue AS (
						SELECT inv.film_id, sum(pay.amount) film_revenue, date_trunc('quarter', current_date_of_the_year) AS current_quarter_of_year
						FROM public.inventory inv
						LEFT JOIN public.rental rnt ON inv.inventory_id = rnt.inventory_id 
						LEFT JOIN public.payment pay ON pay.rental_id = rnt.rental_id 
						WHERE payment_date >= date_trunc('quarter', current_date_of_the_year)  
  								AND payment_date <  date_trunc('quarter', current_date_of_the_year)+ interval '3 month' 
						GROUP BY inv.film_id
)

SELECT flmc.cat_name AS film_category,
	   sum(ftr.film_revenue) AS revenue_for_quarter,
	   EXTRACT(YEAR FROM ftr.current_quarter_of_year) AS current_year,
	   EXTRACT(QUARTER FROM ftr.current_quarter_of_year) as current_quarter
FROM film_category_name flmc
INNER JOIN film_total_revenue ftr ON ftr.film_id = flmc.film_id --inner join should only display categories with at least one sale in the current quarter
GROUP BY flmc.cat_name, ftr.current_quarter_of_year
$$;

--check

SELECT*
FROM get_sales_revenue_by_category_qtr('2017-05-01'); --current_date  --'2017-05-01'

/*Task 3. Create procedure language functions. Create a function that takes a country as an input parameter and returns the most popular film in that specific country. 
The function should format the result set as follows:
                    Query (example):select * from core.most_popular_films_by_countries(array['Afghanistan','Brazil','United States’]);
*/
CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE FUNCTION core.most_popular_films_by_countries(IN countries text[] DEFAULT null)
RETURNS TABLE (country 		text,
			   film			text,
			   rating 		mpaa_rating,
			   "language"	character(20),
			   "length"     smallint,
			   release_year integer)
LANGUAGE plpgsql
AS
$$
BEGIN 

IF countries IS NULL OR coalesce(array_length(countries,1),0) = 0 THEN --returns empty table
    
    RETURN;
END IF;
--an option with error messg
 /*IF countries IS NULL OR coalesce(array_length(countries,1),0) = 0 
    THEN
        RAISE EXCEPTION
            'Parameter "countries" must not be NULL or empty. Pass at least one country name.'
            USING ERRCODE = '22023'; -- invalid_parameter_value
    END IF;*/

    RETURN QUERY
--connect customer=country
WITH customer_country AS (
							SELECT cust.customer_id,
								   cntr.country
							FROM public.customer cust
							INNER JOIN public.address addr ON cust.address_id = addr.address_id 
							INNER JOIN public.city city ON city.city_id = addr.city_id
							INNER JOIN public.country cntr ON cntr.country_id = city.country_id 
							WHERE cntr.country IN (SELECT unnest(countries))
),--list of films with their rental_rate, grouped by country and film
 rating_list AS (			SELECT csc.country,
								   ivn.film_id,
								   count(rnt.rental_id) AS rent_rate
							FROM public.inventory ivn
							INNER JOIN public.rental rnt ON ivn.inventory_id = rnt.inventory_id 
							INNER JOIN customer_country csc ON csc.customer_id = rnt.customer_id
							GROUP BY csc.country, ivn.film_id
							ORDER BY rent_rate desc
)
SELECT ral.country AS country,
	   flm.title AS film,
	   flm.rating AS rating,
	   lan."name" AS "language",
	   flm."length" AS "length",
	   flm.release_year ::integer AS release_year
FROM rating_list ral
INNER JOIN public.film flm ON flm.film_id = ral.film_id
INNER JOIN public."language" lan ON lan.language_id = flm.language_id 
WHERE ral.rent_rate = ( --in case we have several film with the same rate=>to show all them
    SELECT MAX(rent_rate)
    FROM rating_list rl
	WHERE rl.country = ral.country
);
END;
$$;

--check
SELECT * 
FROM core.most_popular_films_by_countries(ARRAY ['Brazil','Afghanistan','United States']); --also tried without any param () and with empty array [array[]::text[]  --gives empty table
--Afghanistan here we have 18 films with the same rental_rate

/*Task 4. Create procedure language functions
Create a function that generates a list of movies available in stock based on a partial title match (e.g., movies containing the word 'love' in their title). 
The titles of these movies are formatted as '%...%', and if a movie with the specified title is not in stock, return a message indicating that it was not found.
The function should produce the result set in the following format (note: the 'row_num' field is an automatically generated counter field, starting from 1 and incrementing for each entry, e.g., 1, 2, ..., 100, 101, ...).

                    Query (example):select * from core.films_in_stock_by_title('%love%’);
*/

CREATE OR REPLACE FUNCTION core.films_in_stock_by_title(IN pattern text DEFAULT null)
RETURNS TABLE ( row_num      bigint,
			    film_title   text,
			    "language"   character(20),
			    customer_name text,
			    rental_date  text
			   )
LANGUAGE plpgsql
AS
$$
BEGIN 

	IF pattern IS NULL
	       OR length(trim(pattern)) = 0 THEN
	        RAISE EXCEPTION
	            'Parameter "pattern" must not be NULL or empty. Pass something like %%love%%';	            
	    END IF;

    RETURN QUERY
   --main query
 	WITH last_rent AS (
				   SELECT inv.inventory_id AS last_inven,
				   		  max(rnt.rental_date) AS last_rent_date
				   FROM public.inventory inv
				   INNER JOIN public.rental rnt ON rnt.inventory_id = inv.inventory_id
				   GROUP BY inv.inventory_id
	)					
   SELECT 	ROW_NUMBER() OVER (ORDER BY flm.title) AS row_num,
   			flm.title AS film_title,
   		  	lan."name" AS "language",
	  		cst.first_name AS customer_name,
	  		to_char(rnt.rental_date,'YYYY-MM-DD HH24:MI:SS') AS rental_date
   FROM public.inventory inv
   INNER JOIN last_rent lrn ON lrn.last_inven = inv.inventory_id 
   INNER JOIN public.film flm ON inv.film_id = flm.film_id
   INNER JOIN public."language" lan ON flm.language_id = lan.language_id
   INNER JOIN public.rental rnt ON rnt.inventory_id = inv.inventory_id AND rnt.rental_date = lrn.last_rent_date
   INNER JOIN public.customer cst ON cst.customer_id = rnt.customer_id
   WHERE flm.title ILIKE pattern
   ORDER BY film_title;
  	
   IF NOT FOUND THEN
        RAISE EXCEPTION
            'No films found for title pattern: %', pattern;           
    END IF;
END;
$$;

--check
select * from core.films_in_stock_by_title('%love%');  --checked with '%hhhhh%', check with empty param and empty string ('')

/*Task 5. Create procedure language functions
Create a procedure language function called 'new_movie' that takes a movie title as a parameter and inserts a new movie with the given title in the film table.
 The function should generate a new unique film ID, set the rental rate to 4.99, the rental duration to three days, the replacement cost to 19.99.
  The release year and language are optional and by default should be current year and Klingon respectively. The function should also verify that the language exists in the 'language' table.
   Then, ensure that no such function has been created before; if so, replace it.*/


CREATE OR REPLACE FUNCTION core.new_movie (
	f_title         text		 DEFAULT NULL,
    f_release_year  integer      DEFAULT EXTRACT(YEAR FROM CURRENT_DATE)::int,
    f_language_name text         DEFAULT 'Klingon')
RETURNS void --doesn't return anything
LANGUAGE plpgsql
AS 
$$
DECLARE f_language_id  integer;

BEGIN
--if title is empty
	IF f_title IS NULL OR btrim(f_title) = '' THEN
        RAISE EXCEPTION 'Movie title must not be NULL or empty';
    END IF;

--check if we have such language and get id
	SELECT lan.language_id
	INTO f_language_id
	FROM public."language" lan
	WHERE lower(lan.name) = lower(f_language_name);

--when we don't have entered language insert default value into public.language and get id
	IF f_language_id IS NULL THEN
	    INSERT INTO public."language"("name")
	    VALUES (f_language_name)
	    RETURNING language_id INTO f_language_id;
	END IF;

	WITH new_film AS (SELECT f_title AS film_title,
							 4.99 AS rental_rate,
							 3 AS rental_duration,
							 19.99 AS replacement_cost,
							 f_language_name AS film_language,
							 f_release_year AS release_year
					)
	
	INSERT INTO public.film (title, release_year, language_id, rental_duration, rental_rate, replacement_cost)
	SELECT nfl.film_title,
		   nfl.release_year,
		   f_language_id,
		   nfl.rental_duration,
		   nfl.rental_rate,
		   nfl.replacement_cost
	FROM new_film nfl
	WHERE NOT EXISTS (SELECT 1
					  FROM public.film flm
					  WHERE flm.title = nfl.film_title
					  AND flm.release_year = nfl.release_year);

END;
$$;

SELECT core.new_movie('Love in Space');
SELECT core.new_movie('Kyiv Stories', 2022);
SELECT core.new_movie('Kyiv', 2025, 'English');
SELECT core.new_movie('Lviv', 2020, 'Ukrainian');
SELECT core.new_movie('');
SELECT core.new_movie();

--check
SELECT * 
FROM film f 
WHERE title IN ('Love in Space', 'Kyiv Stories', 'Kyiv', 'Lviv') 

SELECT* FROM "language"
