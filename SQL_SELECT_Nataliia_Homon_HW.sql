--Part 1: Write SQL queries to retrieve the following data. 

/*PART 1.1 The marketing team needs a list of animation movies between 2017 and 2019 to promote family-friendly content in an upcoming season in stores.
 Show all animation movies released during this period with rate more than 1, sorted alphabetically*/
--	OPTION 1 JOIN (prefer this one option is shorter and believe is good enough)
SELECT fl.title AS film_title
FROM public.film_category fcat                                            --I used an INNER JOIN on three tables to find matching records:the category table contains the category names, the film table contains the film titles, and the film_category table serves as a bridge table between them, representing a many-to-many relationship.
INNER JOIN public.category cat ON fcat.category_id  = cat.category_id 
INNER JOIN public.film fl ON fcat.film_id = fl.film_id 
WHERE LOWER(cat.name) = 'animation'                                 --here in postgre ilike can be used to, but more universal will be the first option
	AND (fl.release_year BETWEEN 2017 AND 2019)                            --(release_year>=2017 and release_year <=2019) other option how to filter
	AND fl.rental_rate > 1
ORDER BY fl.title;

--OPTION 2 SUBQUERIES
SELECT fl.title AS film_title
FROM public.film fl
WHERE fl.film_id IN (SELECT fcat.film_id
					 FROM public.film_category fcat
					 INNER JOIN public.category cat ON fcat.category_id  = cat.category_id
					 WHERE LOWER(cat.name) = 'animation')
AND (fl.release_year BETWEEN 2017 AND 2019)
AND fl.rental_rate > 1
ORDER BY fl.title;

--OPTION 3 CTE
WITH categ_animation AS ( SELECT fcat.film_id AS film_id,
								 fl.title AS film_title,
								 fl.release_year AS release_year,
								 fl.rental_rate AS rental_rate
							 FROM public.film_category fcat
							 INNER JOIN public.category cat ON fcat.category_id  = cat.category_id
							 INNER JOIN public.film fl ON fcat.film_id= fl.film_id 
							 WHERE LOWER(cat.name) = 'animation')

SELECT catanim.film_title 
FROM categ_animation catanim
WHERE catanim.release_year BETWEEN 2017 AND 2019
AND catanim.rental_rate > 1
ORDER BY catanim.film_title;

/*PART 1.2The finance department requires a report on store performance to assess profitability and plan resource allocation for stores after March 2017.
  Calculate the revenue earned by each rental store after March 2017 (since April) (include columns: address and address2 – as one column, revenue)*/

--OPTION 1 CTE
/*The CTE stores information about store addresses. 
It was used to improve the readability of the code and to avoid displaying the store_id in the SELECT clause when grouping data.*/
WITH full_store_addr AS (                        --cte vs subquery => cte is more readable
	SELECT str.store_id,
		CASE
			WHEN addr.address2 IS NOT NULL THEN  addr.address || ', '|| addr.address2  --if there are two addresses in the database, it will merge the two names
			ELSE addr.address END AS store_address
	FROM public.store str
	INNER JOIN public.address addr ON str.address_id  = addr.address_id
)

SELECT 	fst.store_address,
		sum(pay.amount) AS revenue
FROM full_store_addr fst
INNER JOIN public.inventory inv  ON fst.store_id  = inv.store_id        --merge tables with matching data
INNER JOIN public.rental rent ON  rent.inventory_id = inv.inventory_id
INNER JOIN public.payment pay ON  pay.rental_id = rent.rental_id
WHERE  CAST (pay.payment_date AS date) >= '2017-04-01'       --cast for extraction date from timestamp
GROUP BY fst.store_address
ORDER BY revenue;

--OPTION 2 SUBQUERIES
SELECT 	fulladdr.store_address,
		sum(pay.amount) AS revenue
FROM (  SELECT str.store_id,
			CASE
				WHEN addr.address2 IS NOT NULL THEN  addr.address || ', '|| addr.address2  --if there are two addresses in the database, it will merge the two names
				ELSE addr.address END AS store_address
		FROM public.store str
		INNER JOIN public.address addr ON str.address_id  = addr.address_id
	 ) AS fulladdr
INNER JOIN public.inventory inv  ON fulladdr.store_id  = inv.store_id        --merge tables with matching data
INNER JOIN public.rental rent ON  rent.inventory_id = inv.inventory_id
INNER JOIN public.payment pay ON  pay.rental_id = rent.rental_id
WHERE  CAST (pay.payment_date AS date) >= '2017-04-01'       --cast for extraction date from timestamp
GROUP BY fulladdr.store_address
ORDER BY revenue;

--OPTION 3 JOINS
SELECT 	CASE
			WHEN addr.address2 IS NOT NULL THEN  addr.address || ', '|| addr.address2  --if there are two addresses in the database, it will merge the two names
			ELSE addr.address END AS store_address,
		sum(pay.amount) AS revenue
FROM public.store str
INNER JOIN public.address addr ON str.address_id  = addr.address_id
INNER JOIN public.inventory inv  ON str.store_id  = inv.store_id        --merge tables with matching data
INNER JOIN public.rental rent ON  rent.inventory_id = inv.inventory_id
INNER JOIN public.payment pay ON  pay.rental_id = rent.rental_id
WHERE  CAST (pay.payment_date AS date) >= '2017-04-01'       --cast for extraction date from timestamp
GROUP BY addr.address, addr.address2
ORDER BY revenue;


/* PART1.3 
The marketing department in o[ur stores aims to identify the most successful actors since 2015 to boost customer interest in their films.
 Show top-5 actors by number of movies (released after 2015) they took part in
 (columns: first_name, last_name, number_of_movies, sorted by number_of_movies in descending order)*/

--N.B! several actors may have the same number of films in which they have played, so we cannot simply choose the first 5 from the list
--creat a list of actors and the number of films they have appeared in

--OPTION 1 CTE
WITH film_count AS (
	SELECT 	act.first_name,
			act.last_name,
			count(flm.title) AS number_of_movies
	FROM  public.film_actor flma 
	INNER JOIN public.film flm ON flma.film_id = flm.film_id 
	INNER JOIN public.actor act ON flma.actor_id= act.actor_id
	WHERE flm.release_year > 2015
	GROUP BY act.last_name, act.first_name
),

--we determine the top 5 by the number of positions (i.e. we group the data by the number of films), since we cannot use the window function rank
top_five_number as(
	SELECT flmc.number_of_movies AS top_five_rate
	FROM film_count flmc
	GROUP BY flmc.number_of_movies
	ORDER BY flmc.number_of_movies DESC
	LIMIT 5
)

--we compare whether the number of films is included in our artificial separate list of the number of films "top_five_number"
SELECT flmc.first_name,
	   flmc.last_name,
	   flmc.number_of_movies
FROM film_count flmc
WHERE flmc.number_of_movies IN (	SELECT top_five_rate 
								FROM top_five_number)
ORDER BY flmc.number_of_movies DESC;

--OPTION 2 SUBQUERIES

SELECT first_name,
	   last_name,
	   number_of_movies
FROM (	SELECT 	act.first_name,
				act.last_name,
				count(flm.title) AS number_of_movies
		FROM  public.film_actor flma 
		INNER JOIN public.film flm ON flma.film_id = flm.film_id 
		INNER JOIN public.actor act ON flma.actor_id= act.actor_id
		WHERE flm.release_year > 2015
		GROUP BY act.last_name, act.first_name) AS actor_list
WHERE number_of_movies IN ( SELECT distinct count(flm.title) AS number_of_movies
							FROM  public.film_actor flma 
							INNER JOIN public.film flm ON flma.film_id = flm.film_id
							WHERE flm.release_year > 2015
							GROUP BY flma.actor_id
							ORDER BY number_of_movies DESC
							LIMIT 5)

ORDER BY number_of_movies DESC;

--OPTION 3 JOINS - I believe that in this case we can't use only joins without CTE or subqueries or window functions

/*PART 1.4 The marketing team needs to track the production trends of Drama, Travel, and Documentary films to inform genre-specific marketing strategies. 
 Ырщц number of Drama, Travel, Documentary per year (include columns: release_year, number_of_drama_movies, number_of_travel_movies, number_of_documentary_movies),
  sorted by release year in descending order. Dealing with NULL values is encouraged)*/
--COALESCE will help to change NULL to 0

--OPTION 1 CTE
WITH films_cat_list AS (
    SELECT 
        flm.release_year,
        cat.name AS category_name,
        COUNT(flm.film_id) AS number_of_movies
    FROM public.film AS flm
    INNER JOIN public.film_category AS fcat ON flm.film_id = fcat.film_id
    INNER JOIN public.category AS cat ON fcat.category_id = cat.category_id
    WHERE cat.name IN ('Drama','Travel','Documentary')
    GROUP BY flm.release_year, cat.name
)
SELECT 	fcatl.release_year,
		COALESCE(SUM(CASE WHEN fcatl.category_name = 'Drama' THEN fcatl.number_of_movies END),0) AS number_of_drama_movies,
    	COALESCE(SUM(CASE WHEN fcatl.category_name = 'Travel' THEN fcatl.number_of_movies END),0) AS number_of_travel_movies,
    	COALESCE(SUM(CASE WHEN fcatl.category_name = 'Documentary' THEN fcatl.number_of_movies END),0) AS number_of_documentary_movies
FROM films_cat_list fcatl
GROUP BY fcatl.release_year
ORDER BY fcatl.release_year DESC;

--OPTION 2 SUBQUERIES
SELECT 	fcatl.release_year,
		COALESCE(SUM(CASE WHEN fcatl.category_name = 'Drama' THEN fcatl.number_of_movies END),0) AS number_of_drama_movies,
    	COALESCE(SUM(CASE WHEN fcatl.category_name = 'Travel' THEN fcatl.number_of_movies END),0) AS number_of_travel_movies,
    	COALESCE(SUM(CASE WHEN fcatl.category_name = 'Documentary' THEN fcatl.number_of_movies END),0) AS number_of_documentary_movies
FROM (SELECT 
        flm.release_year,
        cat.name AS category_name,
        COUNT(flm.film_id) AS number_of_movies
    FROM public.film AS flm
    INNER JOIN public.film_category AS fcat ON flm.film_id = fcat.film_id
    INNER JOIN public.category AS cat ON fcat.category_id = cat.category_id
    WHERE cat.name IN ('Drama','Travel','Documentary')
    GROUP BY flm.release_year, cat.name
    ) AS fcatl
GROUP BY fcatl.release_year
ORDER BY fcatl.release_year DESC;

--OPTION 3 JOIN
SELECT 	flm.release_year,
		COALESCE(COUNT(CASE WHEN cat.name = 'Drama' THEN flm.film_id  END),0) AS number_of_drama_movies,
    	COALESCE(COUNT(CASE WHEN cat.name = 'Travel' THEN flm.film_id  END),0) AS number_of_travel_movies,
    	COALESCE(COUNT(CASE WHEN cat.name = 'Documentary' THEN flm.film_id  END),0) AS number_of_documentary_movies
FROM public.film_category fcat                                            
INNER JOIN public.category cat ON fcat.category_id  = cat.category_id 
INNER JOIN public.film flm ON fcat.film_id = flm.film_id 
WHERE LOWER(cat.name) = 'drama' OR LOWER(cat.name) = 'travel' OR LOWER(cat.name) = 'documentary'
GROUP BY flm.release_year
ORDER BY flm.release_year DESC;

--Part 2: Solve the following problems using SQL

/*PART 2.1The HR department aims to reward top-performing employees in 2017 with bonuses to recognize their contribution to stores revenue. Show which three employees generated the most revenue in 2017? 

Assumptions: 
staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
if staff processed the payment then he works in the same store; 
take into account only payment_date
*/
--reflect the income for each employee who received payment for renting movies, taking into account which store the payment was made at. Show the top 3
--checked that store_id in table staff coresponds to the latest update

--OPTION 1 CTE
WITH store_revenue AS (
			SELECT  inv.store_id AS store_recieved_revenue,
					pay.staff_id AS staff_recived_payment,
					sum(pay.amount) AS revenue
			FROM public.rental rent
			INNER JOIN public.inventory inv ON rent.inventory_id = inv.inventory_id --from which store rental is performed, will take into acount this store_id
			INNER JOIN public.payment pay ON rent.rental_id = pay.rental_id --take into acount staff_id who recieved payment +calculate payments, r.staff_id may differ from p.staff_id
			WHERE EXTRACT (YEAR FROM pay.payment_date ) = 2017
			GROUP BY inv.store_id, pay.staff_id
			)

SELECT  stf.staff_id,
		stf.first_name || ' ' || stf.last_name AS full_name,
		strev.store_recieved_revenue,
		strev.revenue,
		stf.store_id AS currect_staff_store		
FROM store_revenue strev
INNER JOIN public.staff stf ON strev.staff_recived_payment = stf.staff_id --here will take into acount current store_id of the staff
ORDER BY strev.revenue DESC
LIMIT 3;
--in case store address is needed it could be added with the help of JOIN table 'address'

--OPTION 2 SUBQUERIES
SELECT  stf.staff_id,
		stf.first_name || ' ' || stf.last_name AS full_name,
		strev.store_recieved_revenue,
		strev.revenue,
		stf.store_id AS currect_staff_store		
FROM 	(SELECT  inv.store_id AS store_recieved_revenue,
					pay.staff_id AS staff_recived_payment,
					sum(pay.amount) AS revenue
		 FROM public.rental rent
		 INNER JOIN public.inventory inv ON rent.inventory_id = inv.inventory_id --from which store rental is performed, will take into acount this store_id
		 INNER JOIN public.payment pay ON rent.rental_id = pay.rental_id --take into acount staff_id who recieved payment +calculate payments, r.staff_id may differ from p.staff_id
		 WHERE EXTRACT (YEAR FROM pay.payment_date ) = 2017
		 GROUP BY inv.store_id, pay.staff_id
		) AS strev
INNER JOIN public.staff stf ON strev.staff_recived_payment = stf.staff_id --here will take into acount current store_id of the staff
ORDER BY strev.revenue DESC
LIMIT 3;

--OPTION 3 JOIN
SELECT  pay.staff_id AS staff_id,
		stf.first_name || ' ' || stf.last_name AS full_name,
		inv.store_id AS store_recieved_revenue,
		SUM(pay.amount) AS revenue,
		stf.store_id AS current_staff_store		
FROM 	 public.rental rent
		 INNER JOIN public.inventory inv ON rent.inventory_id = inv.inventory_id --from which store rental is performed, will take into acount this store_id
		 INNER JOIN public.payment pay ON rent.rental_id = pay.rental_id --take into acount staff_id who recieved payment +calculate payments, r.staff_id may differ from p.staff_id
		 INNER JOIN public.staff stf ON pay.staff_id = stf.staff_id
WHERE EXTRACT (YEAR FROM pay.payment_date ) = 2017
GROUP BY inv.store_id, pay.staff_id, stf.store_id, stf.first_name || ' ' || stf.last_name
ORDER BY revenue DESC
LIMIT 3;

/* PART 2.2. The management team wants to identify the most popular movies and their target audience age groups to optimize marketing efforts.
 *  Show which 5 movies were rented more than others (number of rentals), and what's the expected age of the audience for these movies?
 *  To determine expected age please use 'Motion Picture Association film rating system'*/

--NB! Multiple movies can have the same rental amount
--form list of films with amount of rentals
--OPTION 1 CTE
WITH rental_rate_list AS (
							SELECT  inv.film_id,
									count(rent.rental_id) AS film_rental_rate
							FROM public.inventory inv
							INNER JOIN public.rental rent ON inv.inventory_id = rent.inventory_id
							GROUP BY inv.film_id
							),
--since we have several movies with the same number of rentals we need to create an artificial list - top 5 values ​​by number of rentals
rental_rate_five AS (
							SELECT rrl.film_rental_rate AS top_five_rate
							FROM rental_rate_list rrl
							GROUP BY rrl.film_rental_rate
							ORDER BY rrl.film_rental_rate DESC
							LIMIT 5
							)

--we compare whether the number of rentals is included in our artificial separate list of the number of films "rental_rate_five"
SELECT 	flm.title AS film_title,
		rrl.film_rental_rate,
		CASE 	WHEN flm.rating ='G' THEN 'All ages admitted'
		 		WHEN flm.rating = 'PG' THEN 'Around 8+ (with parental guidance)'
		 		WHEN flm.rating = 'PG-13' THEN '13 and older'
		 		WHEN flm.rating = 'R' THEN '17 and older (or with parent/guardian)'
		 		WHEN flm.rating = 'NC-17' THEN '18+' END AS age_movie_recommendation	   		  
FROM rental_rate_list rrl
INNER JOIN public.film flm ON rrl.film_id = flm.film_id 
WHERE rrl.film_rental_rate IN (	SELECT top_five_rate 
								FROM rental_rate_five)
ORDER BY rrl.film_rental_rate DESC, film_title asc;

--OPTION 2 SUBQUERIES
SELECT 	flm.title AS film_title,
		rrl.film_rental_rate,
		CASE 	WHEN flm.rating ='G' THEN 'All ages admitted'
		 		WHEN flm.rating = 'PG' THEN 'Around 8+ (with parental guidance)'
		 		WHEN flm.rating = 'PG-13' THEN '13 and older'
		 		WHEN flm.rating = 'R' THEN '17 and older (or with parent/guardian)'
		 		WHEN flm.rating = 'NC-17' THEN '18+' END AS age_movie_recommendation	   		  
FROM (SELECT  inv.film_id,
	  count(rent.rental_id) AS film_rental_rate
	  FROM public.inventory inv
	  INNER JOIN public.rental rent ON inv.inventory_id = rent.inventory_id
	  GROUP BY inv.film_id) AS rrl
INNER JOIN public.film flm ON rrl.film_id = flm.film_id 
WHERE rrl.film_rental_rate >= (	SELECT min(film_rate.film_rental_rate) 
								FROM    (SELECT  DISTINCT count(rent.rental_id) AS film_rental_rate
			  							FROM public.inventory inv
			  							INNER JOIN public.rental rent ON inv.inventory_id = rent.inventory_id
			  							GROUP BY inv.film_id
			  							ORDER BY film_rental_rate DESC
			  							limit 5) AS film_rate
			  					)
ORDER BY rrl.film_rental_rate DESC, film_title asc;

--OPTION 3 JOINS I believe that in this case we can't use only joins without CTE or subqueries or window functions. 

--Part 3. Which actors/actresses didn't act for a longer period of time than the others? 

/*PART 3.1 The stores’ marketing team wants to analyze actors' inactivity periods to select those with notable career breaks for targeted promotional campaigns, highlighting their comebacks or consistent appearances to engage customers with nostalgic or reliable film stars
The task can be interpreted in various ways, and here are a few options (provide solutions for each one):
V1: gap between the latest release_year and current year per each actor;
V2: gaps between sequential films per each actor;
*/

--V1: gap between the latest release_year and current year per each actor;
--OPTION 1 CTE 
WITH actor_list AS (SELECT 	act.actor_id AS actor_id,
							act.first_name || ' '|| act.last_name AS actor_full_name,
							MAX(flm.release_year) AS latest_release_year
					FROM public.film_actor flma 					
					INNER JOIN public.actor act ON flma.actor_id  = act.actor_id
					INNER JOIN public.film flm ON flma.film_id = flm.film_id 
					GROUP BY  act.actor_id, act.first_name, act.last_name
					)
SELECT 	actl.actor_id,
		actl.actor_full_name,
		actl.latest_release_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP) AS current_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP)- actl.latest_release_year AS break_interval
FROM actor_list actl	
ORDER BY actl.actor_id;

--OPTION 2 SUBQUERIES
SELECT 	actl.actor_id,
		actl.actor_full_name,
		actl.latest_release_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP) AS current_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP)- actl.latest_release_year AS break_interval
FROM (SELECT 	act.actor_id AS actor_id,
							act.first_name || ' '|| act.last_name AS actor_full_name,
							MAX(flm.release_year) AS latest_release_year
					FROM public.film_actor flma 					
					INNER JOIN public.actor act ON flma.actor_id  = act.actor_id
					INNER JOIN public.film flm ON flma.film_id = flm.film_id 
					GROUP BY  act.actor_id, act.first_name, act.last_name)	AS actl
ORDER BY actl.actor_id;

--OPTION 3 JOIN
SELECT 	act.actor_id,
		act.first_name || ' '|| act.last_name AS actor_full_name,
		MAX(flm.release_year) AS latest_release_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP) AS current_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP)- MAX(flm.release_year) AS break_interval
FROM public.film_actor flma 
INNER JOIN public.film flm ON flma.film_id = flm.film_id 
INNER JOIN public.actor act ON flma.actor_id  = act.actor_id 
GROUP BY  act.actor_id, act.first_name, act.last_name
ORDER BY act.actor_id;

--V2: gaps between sequential films per each actor;
--OPTION 1 CTE example with NULL value

WITH actor_years AS (   SELECT  act.actor_id,
						        act.first_name || ' ' || act.last_name AS actor_full_name,
						        flm.release_year
					    FROM public.actor AS act
					    INNER JOIN public.film_actor AS flma
					        ON act.actor_id = flma.actor_id
					    INNER JOIN public.film AS flm
					        ON flma.film_id = flm.film_id
					    GROUP BY act.actor_id, act.first_name, act.last_name, flm.release_year
)
SELECT
    acty.actor_full_name,
    acty.release_year AS prev_release_year,
    MIN(acty2.release_year) AS next_release_year,
    CASE 
        WHEN MIN(acty2.release_year) IS NULL THEN NULL
        ELSE MIN(acty2.release_year) - acty.release_year
    END AS gap_between_films
FROM actor_years AS acty
LEFT OUTER JOIN actor_years AS acty2 ON acty2.actor_id = acty.actor_id AND acty2.release_year > acty.release_year
GROUP BY acty.actor_full_name, acty.release_year
ORDER BY acty.actor_full_name ASC, acty.release_year ASC;

					

--OPTION 2 SUBQUERIES example with NULL
SELECT  prev.actor_full_name,
	    prev.prev_release_year,
	    MIN(nexts.release_year) AS next_release_year,
	    CASE 
	        WHEN MIN(nexts.release_year) IS NULL THEN NULL
	        ELSE MIN(nexts.release_year) - prev.prev_release_year
	    END AS gap_between_films
FROM (
    SELECT
        act.actor_id,
        act.first_name || ' ' || act.last_name AS actor_full_name,
        flm.release_year AS prev_release_year
    FROM public.actor AS act
    INNER JOIN public.film_actor AS flma ON act.actor_id = flma.actor_id
    INNER JOIN public.film AS flm ON flma.film_id = flm.film_id
    GROUP BY act.actor_id, act.first_name, act.last_name, flm.release_year
	) AS prev
LEFT OUTER JOIN (
    SELECT
        act2.actor_id,
        flm2.release_year
    FROM public.actor AS act2
    INNER JOIN public.film_actor AS flma2 ON act2.actor_id = flma2.actor_id
    INNER JOIN public.film AS flm2 ON flma2.film_id = flm2.film_id
    GROUP BY act2.actor_id, flm2.release_year
) AS nexts
    ON nexts.actor_id = prev.actor_id
   AND nexts.release_year > prev.prev_release_year
GROUP BY prev.actor_full_name, prev.prev_release_year
ORDER BY prev.actor_full_name ASC, prev.prev_release_year ASC;

--OPTION 3 JOIN years data type changed to text type
SELECT 
    act.first_name || ' '|| act.last_name AS actor_full_name,
    flm1.release_year AS prev_release_year,
    COALESCE (MIN(flm2.release_year)::text, '-') AS next_release_year,  --change NULL into '-', !NB data type is changed to text, if numeric value is required it should be changed to 0/NULL
    COALESCE ((MIN(flm2.release_year) - flm1.release_year)::text, '-') AS gap_between_films
FROM public.actor act
JOIN public.film_actor fa1 ON act.actor_id = fa1.actor_id
JOIN public.film flm1 ON fa1.film_id = flm1.film_id
LEFT OUTER JOIN public.film_actor fa2 ON act.actor_id = fa2.actor_id
LEFT OUTER JOIN public.film flm2 ON fa2.film_id = flm2.film_id AND flm2.release_year > flm1.release_year  --to display the latest movie release, after which there are no others
GROUP BY act.first_name, act.last_name, flm1.release_year
ORDER BY  actor_full_name, flm1.release_year;