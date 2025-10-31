--Part 1: Write SQL queries to retrieve the following data. 

/*PART 1.1 The marketing team needs a list of animation movies between 2017 and 2019 to promote family-friendly content in an upcoming season in stores.
 Show all animation movies released during this period with rate more than 1, sorted alphabetically*/

SELECT f.title AS film_title
FROM public.film_category fc                                            --I used an INNER JOIN on three tables to find matching records:the category table contains the category names, the film table contains the film titles, and the film_category table serves as a bridge table between them, representing a many-to-many relationship.
INNER JOIN public.category c ON fc.category_id  = c.category_id 
INNER JOIN public.film f ON fc.film_id = f.film_id 
WHERE LOWER(c.name) = 'animation'                                 --here in postgre ilike can be used to, but more universal will be the first option
	AND (release_year BETWEEN 2017 AND 2019)                            --(release_year>=2017 and release_year <=2019) other option how to filter
	AND f.rental_rate > 1
ORDER BY f.title;


/*PART 1.2The finance department requires a report on store performance to assess profitability and plan resource allocation for stores after March 2017.
  Calculate the revenue earned by each rental store after March 2017 (since April) (include columns: address and address2 – as one column, revenue)*/

/*The CTE stores information about store addresses. 
It was used to improve the readability of the code and to avoid displaying the store_id in the SELECT clause when grouping data.*/
WITH full_store_addr AS (                        --cte vs subquery => cte is more readable
	SELECT s.store_id,
		CASE
			WHEN a.address2 IS NOT NULL THEN  a.address || ', '|| a.address2  --if there are two addresses in the database, it will merge the two names
			ELSE a.address END AS store_address
	FROM public.store s
	INNER JOIN public.address a ON s.address_id  = a.address_id
)

SELECT 	st.store_address,
		count(p.amount) AS revenue
FROM full_store_addr st
INNER JOIN public.inventory i  ON st.store_id  = i.store_id        --merge tables with matching data
INNER JOIN public.rental r ON  r.inventory_id = i.inventory_id
INNER JOIN public.payment p ON  p.rental_id = r.rental_id
WHERE  CAST (p.payment_date AS date) >= '2017-04-01'       --cast for extraction date from timestamp
GROUP BY st.store_address
ORDER BY revenue;


/* PART1.3 
The marketing department in our stores aims to identify the most successful actors since 2015 to boost customer interest in their films.
 Show top-5 actors by number of movies (released after 2015) they took part in
 (columns: first_name, last_name, number_of_movies, sorted by number_of_movies in descending order)*/

--N.B! several actors may have the same number of films in which they have played, so we cannot simply choose the first 5 from the list
--creat a list of actors and the number of films they have appeared in
WITH film_count AS (
	SELECT 	a.first_name,
			a.last_name,
			count(f.title) AS number_of_movies
	FROM  public.film_actor fa 
	INNER JOIN public.film f ON fa.film_id = f.film_id 
	INNER JOIN public.actor a ON fa.actor_id= a.actor_id
	WHERE f.release_year > 2015
	GROUP BY a.last_name, a.first_name
),

--we determine the top 5 by the number of positions (i.e. we group the data by the number of films), since we cannot use the window function rank
top_five_number as(
	SELECT fc.number_of_movies AS top_five_rate
	FROM film_count fc
	GROUP BY fc.number_of_movies
	ORDER BY fc.number_of_movies DESC
	LIMIT 5
)

--we compare whether the number of films is included in our artificial separate list of the number of films "top_five_number"
SELECT fc.first_name,
	   fc.last_name,
	   fc.number_of_movies
FROM film_count fc
WHERE fc.number_of_movies IN (	SELECT top_five_rate 
								FROM top_five_number)
ORDER BY fc.number_of_movies DESC;

/*PART 1.4 The marketing team needs to track the production trends of Drama, Travel, and Documentary films to inform genre-specific marketing strategies. 
 Ырщц number of Drama, Travel, Documentary per year (include columns: release_year, number_of_drama_movies, number_of_travel_movies, number_of_documentary_movies),
  sorted by release year in descending order. Dealing with NULL values is encouraged)*/
--COALESCE will help to change NULL to 0

SELECT 	release_year,
		COALESCE ((CASE WHEN LOWER(c.name) = 'drama' THEN  count(c.name) END), 0) AS number_of_drama_movies,
		COALESCE ((CASE WHEN LOWER(c.name) = 'travel' THEN  count(c.name) END), 0) AS number_of_travel_movies,
		COALESCE ((CASE WHEN LOWER(c.name) = 'documentary' THEN  count(c.name) END), 0) AS number_of_documentary_movies
FROM public.film_category fc                                            
INNER JOIN public.category c ON fc.category_id  = c.category_id 
INNER JOIN public.film f ON fc.film_id = f.film_id 
WHERE LOWER(c.name) = 'drama' OR LOWER(c.name) = 'travel' OR LOWER(c.name) = 'documentary'
GROUP BY release_year, c.name
ORDER BY release_year DESC

--Part 2: Solve the following problems using SQL

/*PART 2.1The HR department aims to reward top-performing employees in 2017 with bonuses to recognize their contribution to stores revenue. Show which three employees generated the most revenue in 2017? 

Assumptions: 
staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
if staff processed the payment then he works in the same store; 
take into account only payment_date
*/
--reflect the income for each employee who received payment for renting movies, taking into account which store the payment was made at. Show the top 3
--checked that store_id in table staff coresponds to the latest update

WITH store_revenue AS (
			SELECT  i.store_id AS store_recieved_revenue,
					p.staff_id AS staff_recived_payment,
					count(p.amount) AS revenue
			FROM public.rental r
			INNER JOIN public.inventory i ON r.inventory_id = i.inventory_id --from which store rental is performed, will take into acount this store_id
			INNER JOIN public.payment p ON r.rental_id = p.rental_id --take into acount staff_id who recieved payment +calculate payments, r.staff_id may differ from p.staff_id
			WHERE EXTRACT (YEAR FROM p.payment_date ) = 2017
			GROUP BY i.store_id, p.staff_id
			)

SELECT  s.staff_id,
		s.first_name || ' ' || s.last_name AS full_name,
		sr.store_recieved_revenue,
		sr.revenue,
		s.store_id AS currect_staff_store		
FROM store_revenue sr
INNER JOIN public.staff s ON sr.staff_recived_payment = s.staff_id --here will take into acount current store_id of the staff
ORDER BY sr.revenue DESC
LIMIT 3
--in case store address is needed it could be added with the help of JOIN table 'address'


/* PART 2.2. The management team wants to identify the most popular movies and their target audience age groups to optimize marketing efforts.
 *  Show which 5 movies were rented more than others (number of rentals), and what's the expected age of the audience for these movies?
 *  To determine expected age please use 'Motion Picture Association film rating system'*/

--NB! Multiple movies can have the same rental amount
--form list of films with amount of rentals
WITH rental_rate_list AS (
							SELECT  i.film_id,
									count(r.rental_id) AS film_rental_rate
							FROM public.inventory i
							INNER JOIN public.rental r ON i.inventory_id = r.inventory_id
							GROUP BY i.film_id
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
SELECT 	f.title AS film_title,
		rrl.film_rental_rate,
		CASE 	WHEN f.rating ='G' THEN 'All ages admitted'
		 		WHEN f.rating = 'PG' THEN 'Around 8+ (with parental guidance)'
		 		WHEN f.rating = 'PG-13' THEN '13 and older'
		 		WHEN f.rating = 'R' THEN '17 and older (or with parent/guardian)'
		 		WHEN f.rating = 'NC-17' THEN '18+' END AS age_movie_recommendation	   		  
FROM rental_rate_list rrl
INNER JOIN public.film f ON rrl.film_id = f.film_id 
WHERE rrl.film_rental_rate IN (	SELECT top_five_rate 
								FROM rental_rate_five)
ORDER BY rrl.film_rental_rate DESC;

--Part 3. Which actors/actresses didn't act for a longer period of time than the others? 

/*PART 3.1 The stores’ marketing team wants to analyze actors' inactivity periods to select those with notable career breaks for targeted promotional campaigns, highlighting their comebacks or consistent appearances to engage customers with nostalgic or reliable film stars
The task can be interpreted in various ways, and here are a few options (provide solutions for each one):
V1: gap between the latest release_year and current year per each actor;
V2: gaps between sequential films per each actor;
*/

--V1: gap between the latest release_year and current year per each actor;

SELECT 	a.actor_id,
		a.first_name || ' '|| a.last_name AS actor_full_name,
		MAX(f.release_year) AS latest_release_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP) AS current_year,
		EXTRACT(YEAR FROM CURRENT_TIMESTAMP)- MAX(f.release_year) AS break_interval
FROM public.film_actor fa 
INNER JOIN public.film f ON fa.film_id = f.film_id 
INNER JOIN public.actor a ON fa.actor_id  = a.actor_id 
GROUP BY  a.actor_id, a.first_name, a.last_name
ORDER BY a.actor_id;

--V2: gaps between sequential films per each actor;

SELECT 
    a.actor_id,
    a.first_name || ' '|| a.last_name AS actor_full_name,
    f1.release_year AS prev_release_year,
    COALESCE (MIN(f2.release_year)::text, '-') AS next_release_year,  --change NULL into '-', !NB data type is changed to text, if numeric value is required it should be changed to 0/NULL
    COALESCE ((MIN(f2.release_year) - f1.release_year)::text, '-') AS gap_between_films
FROM public.actor a
JOIN public.film_actor fa1 ON a.actor_id = fa1.actor_id
JOIN public.film f1 ON fa1.film_id = f1.film_id
LEFT OUTER JOIN public.film_actor fa2 ON a.actor_id = fa2.actor_id
LEFT OUTER JOIN public.film f2 ON fa2.film_id = f2.film_id AND f2.release_year > f1.release_year  --to display the latest movie release, after which there are no others
GROUP BY a.actor_id, f1.release_year
ORDER BY a.actor_id, f1.release_year;











 


