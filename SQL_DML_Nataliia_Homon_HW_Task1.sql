--	inserting 3 movies into the table films
BEGIN TRANSACTION;

--form virtual-like table using CTE and syntax VALUES in from section, this table by column name(from alias) will be inserted into our database, other option is to use UNION will be shown lower
WITH favorite_films AS (SELECT *FROM 
						   (VALUES
							('Harry Potter and the Philosopher''s Stone', 'A young boy discovers he’s a wizard and begins his magical journey at Hogwarts School of Witchcraft and Wizardry', 2001, 'PG',     4.99::numeric,  7::int, 152::int, 24.99::numeric),
	        				('Star Wars: Episode IV – A New Hope', 'A farm boy joins a rebellion against an evil empire and helps destroy a deadly space station.', 1977, 'PG',     9.99::numeric, 14::int, 121::int, 24.99::numeric),
	        				('The Lord of the Rings: The Fellowship of the Ring', 'A young hobbit embarks on an epic quest to destroy a powerful ring and save Middle-earth.', 2001, 'PG-13', 19.99::numeric, 21::int, 178::int, 39.99::numeric)	
	        				) virt_table (title, description, release_year, rating, rental_rate, rental_duration, length_min, replacement_cost )
	        		)
INSERT INTO public.film  (title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update)
SELECT ffs.title,
  	ffs.description,
    ffs.release_year,
    lang.language_id,
    ffs.rental_duration,
    ffs.rental_rate,
    ffs.length_min,
    ffs.replacement_cost,
    ffs.rating::mpaa_rating,
    CURRENT_DATE
FROM favorite_films AS ffs
INNER JOIN public."language" lang ON lang.name = 'English' --avoid hardcoding, we want to get language_id
WHERE NOT EXISTS (SELECT 1                                --avoud dublicates +reusable
				  FROM public.film existf
				  WHERE existf.title = ffs.title
				  AND existf.release_year = ffs.release_year)
RETURNING film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update; --to see that all films pass NOT EXIST CHECK and were inserted and to show id (for studying purpose)
 
COMMIT;
				  
--inserting actors into table actor
BEGIN TRANSACTION;
--will use CTE with UNION to form a list of actors
WITH fav_film_actors AS (   SELECT 'Daniel' AS first_name, 'Radcliffe' AS last_name
						    UNION ALL SELECT 'Emma', 'Watson'
						    UNION ALL SELECT 'Rupert', 'Grint'
						    UNION ALL SELECT 'Mark', 'Hamill'
						    UNION ALL SELECT 'Harrison', 'Ford'
						    UNION ALL SELECT 'Carrie', 'Fisher'
						    UNION ALL SELECT 'Elijah', 'Wood'
						    UNION ALL SELECT 'Ian', 'McKellen'
						    UNION ALL SELECT 'Viggo', 'Mortensen'
							)
INSERT INTO public.actor (first_name, last_name, last_update)
SELECT ffa.first_name,
	   ffa.last_name,
	   CURRENT_DATE
FROM fav_film_actors ffa
WHERE NOT EXISTS (SELECT 1                    ----avoud dublicates +reusable
				  FROM public.actor existact
				  WHERE existact.first_name = ffa.first_name
				  AND existact.last_name = ffa.last_name)
RETURNING actor_id, first_name, last_name, last_update;

--verifying to dublecheck(studying purpose)
SELECT actor_id, first_name, last_name
FROM public.actor
WHERE (first_name, last_name) IN (
    ('Daniel','Radcliffe'),
    ('Emma','Watson'),
    ('Rupert','Grint'),
    ('Mark','Hamill'),
    ('Harrison','Ford'),
    ('Carrie','Fisher'),
    ('Elijah','Wood'),
    ('Ian','McKellen'),
    ('Viggo','Mortensen')
)
ORDER BY last_name, first_name;

COMMIT;

--adding connection between actors and films in table film_actor
BEGIN TRANSACTION;

WITH film_maping AS(SELECT  flm.film_id,
							flm.title
					FROM public.film flm
					WHERE (flm.title, flm.release_year) IN ( ('Harry Potter and the Philosopher''s Stone', 2001),   --in case in future maybe remakes with the same title=> release_year added
  										 ('Star Wars: Episode IV – A New Hope', 1977),
  										 ('The Lord of the Rings: The Fellowship of the Ring', 2001)
  										 )
),
actor_maping AS (SELECT act.actor_id,
						   act.first_name,
						   act.last_name
					FROM public.actor act
					WHERE (act.first_name, act.last_name) IN ( ('Daniel','Radcliffe'), ('Emma','Watson'), ('Rupert','Grint'),
													        ('Mark','Hamill'), ('Harrison','Ford'), ('Carrie','Fisher'),
													        ('Elijah','Wood'), ('Ian','McKellen'), ('Viggo','Mortensen')
													      )
),
film_actor_connection AS (SELECT * FROM (VALUES
							 				('Harry Potter and the Philosopher''s Stone','Daniel','Radcliffe'),
									        ('Harry Potter and the Philosopher''s Stone','Emma','Watson'),
									        ('Harry Potter and the Philosopher''s Stone','Rupert','Grint'),
									
									        ('Star Wars: Episode IV – A New Hope','Mark','Hamill'),
									        ('Star Wars: Episode IV – A New Hope','Harrison','Ford'),
									        ('Star Wars: Episode IV – A New Hope','Carrie','Fisher'),
									
									        ('The Lord of the Rings: The Fellowship of the Ring','Elijah','Wood'),
									        ('The Lord of the Rings: The Fellowship of the Ring','Ian','McKellen'),
									        ('The Lord of the Rings: The Fellowship of the Ring','Viggo','Mortensen')
								    	) column_name (film_title, first_name, last_name)
						 )
INSERT INTO public.film_actor (actor_id, film_id, last_update)
SELECT  a_map.actor_id,
		flm_map.film_id,
		CURRENT_DATE
FROM film_actor_connection connectfa
INNER JOIN film_maping flm_map ON flm_map.title = connectfa.film_title
INNER JOIN actor_maping a_map ON a_map.first_name = connectfa.first_name AND a_map.last_name = connectfa.last_name
ON CONFLICT DO NOTHING  --rerunnable/reusable
RETURNING actor_id, film_id;

--check table after insert
SELECT flma.actor_id,
	act.first_name,
	act.last_name,
	flma.film_id,
	flm.title
FROM public.film_actor flma
INNER JOIN public.actor act ON act.actor_id = flma.actor_id
INNER JOIN public.film  flm ON flm.film_id = flma.film_id
WHERE flm.title IN (
  'Harry Potter and the Philosopher''s Stone',
  'Star Wars: Episode IV – A New Hope',
  'The Lord of the Rings: The Fellowship of the Ring'
)
ORDER BY flm.title;

COMMIT;

--adding films to inventory table
BEGIN TRANSACTION; 

--find existing store_id
WITH store_new_film AS (SELECT str.store_id 
						FROM store str
						ORDER BY str.store_id
						LIMIT 1)
INSERT INTO public.inventory (film_id, store_id, last_update)
SELECT newfilm.film_id, strnew.store_id, CURRENT_DATE
FROM store_new_film strnew
CROSS JOIN (SELECT flm.film_id,   --use cross join as we don't have connecting columns, just need to add new column with store_id info, subquery - other option vs CTE
				   flm.title
  			FROM public.film flm
  			WHERE flm.title IN (
		    'Harry Potter and the Philosopher''s Stone',
		    'Star Wars: Episode IV – A New Hope',
		    'The Lord of the Rings: The Fellowship of the Ring')
            ) newfilm
WHERE NOT EXISTS (SELECT 1
				  FROM public.inventory inv
				  WHERE inv.film_id = newfilm.film_id
				  AND inv.store_id = strnew.store_id
				)
RETURNING inventory_id, film_id, store_id;

--doble checking table
SELECT inv.inventory_id, inv.film_id, flm.title, inv.store_id
FROM public.inventory inv
INNER JOIN public.film flm ON flm.film_id = inv.film_id
WHERE flm.title IN (
  'Harry Potter and the Philosopher''s Stone',
  'Star Wars: Episode IV – A New Hope',
  'The Lord of the Rings: The Fellowship of the Ring'
);

COMMIT;

-- Change a customer with >=43 rentals and payments; update personal data (no address updates)


BEGIN TRANSACTION;

WITH person_changed AS (SELECT cust.customer_id						       
						FROM public.customer cust
						LEFT JOIN public.rental  rent ON rent.customer_id = cust.customer_id
						LEFT JOIN public.payment pay ON pay.customer_id = cust.customer_id
						GROUP BY cust.customer_id
						HAVING COUNT(DISTINCT rent.rental_id) >= 43
						   AND COUNT(DISTINCT pay.payment_id) >= 43
						ORDER BY COUNT(DISTINCT rent.rental_id) DESC, COUNT(DISTINCT pay.payment_id) DESC  --to find most active customer
						LIMIT 1
)
UPDATE public.customer cust
SET first_name = 'Nataliia',
	last_name  = 'Homon',
    email      = 'nataly.homon@gmail.com',
    address_id = (SELECT addr.address_id       --it could also be presented as CTE
					   FROM public.address addr
					   ORDER BY addr.address_id
					   LIMIT 1),
    last_update = CURRENT_DATE    
FROM person_changed pers    --indicate what customer_id should be changed
WHERE cust.customer_id = pers.customer_id
RETURNING cust.customer_id, cust.first_name, cust.last_name, cust.email, cust.address_id;
 
--doble check
SELECT customer_id, first_name, last_name, email, address_id
FROM public.customer
WHERE first_name = 'Nataliia' AND last_name = 'Homon';

COMMIT;

--delete records from rental and payment tables connectong to changed customer
BEGIN TRANSACTION;

--check if rentals+ payments exist (on basis of payment table)
				
SELECT COUNT(*) AS payments_to_delete
FROM public.payment pay
WHERE pay.customer_id = (SELECT cust.customer_id
				FROM public.customer cust
				WHERE cust.first_name = 'Nataliia' AND cust.last_name = 'Homon');

--delete payments
DELETE FROM public.payment pay
WHERE pay.customer_id = (SELECT cust.customer_id
				FROM public.customer cust
				WHERE cust.first_name = 'Nataliia' AND cust.last_name = 'Homon')
RETURNING pay.payment_id;

--delete rentals
DELETE FROM public.rental rent
WHERE rent.customer_id = (SELECT cust.customer_id
				FROM public.customer cust
				WHERE cust.first_name = 'Nataliia' AND cust.last_name = 'Homon')
RETURNING rent.rental_id;

--one more option is avaliable to create CTE with customer_id info and use DELETE FROM public.payment p USING CTE 
--to doble check deleted payments I run the query above, similar with rentals
COMMIT;

--create new rentals by me
BEGIN TRANSACTION;

WITH mydata AS (
  SELECT customer_id
  FROM public.customer
  WHERE first_name = 'Nataliia' AND last_name = 'Homon'
  LIMIT 1
),
films_to_rent AS (
  SELECT flm.film_id, 
  		flm.title,
  		flm.rental_rate
  FROM public.film flm
  WHERE flm.title IN (
    'Harry Potter and the Philosopher''s Stone',
    'Star Wars: Episode IV – A New Hope',
    'The Lord of the Rings: The Fellowship of the Ring'
  )
),
invent AS (
  SELECT inv.inventory_id,
  		inv.film_id,
  		inv.store_id
  FROM public.inventory inv
  INNER JOIN films_to_rent flm ON flm.film_id = inv.film_id
),
one_staff AS (
  SELECT distinct stf.staff_id
  FROM public.staff stf
  INNER JOIN invent res ON res.store_id = stf.store_id
  LIMIT 1
),
date_maping AS (
  SELECT * FROM (VALUES
    ('Harry Potter and the Philosopher''s Stone', DATE '2017-03-01', DATE '2017-03-10', TIMESTAMP '2017-03-01 10:00:00'),
    ('Star Wars: Episode IV – A New Hope',        DATE '2017-03-02', DATE '2017-03-16', TIMESTAMP '2017-03-02 10:00:00'),
    ('The Lord of the Rings: The Fellowship of the Ring', DATE '2017-03-03', DATE '2017-03-24', TIMESTAMP '2017-03-03 10:00:00')
  ) column_names(title, rent_dt, return_dt, pay_ts)
),
to_rent AS (
  SELECT inv.inventory_id,
  		 inv.film_id,
  		 ftr.title,
  		 dmp.rent_dt,
  		 dmp.return_dt
  FROM invent inv
  INNER JOIN films_to_rent ftr  ON ftr.film_id = inv.film_id
  INNER JOIN date_maping dmp ON dmp.title = ftr.title
)

  INSERT INTO public.rental (rental_date, inventory_id, customer_id, return_date, staff_id, last_update)
  SELECT trn.rent_dt,
  		 trn.inventory_id,
  		 myd.customer_id,
  		 trn.return_dt,
  		 stf.staff_id,
  		 CURRENT_DATE
  FROM to_rent trn
  CROSS JOIN mydata myd
  CROSS JOIN one_staff stf
  WHERE NOT EXISTS (   -- re-runnable
					  SELECT 1
					  FROM public.rental ren
					  WHERE ren.customer_id  = myd.customer_id
					    AND ren.inventory_id = trn.inventory_id
					    AND ren.rental_date  = trn.rent_dt)
  RETURNING rental_id, inventory_id, customer_id, staff_id, rental_date, return_date;
  
--doble check
SELECT * 
FROM rental
WHERE customer_id  = (SELECT customer_id
  					  FROM public.customer
  					  WHERE first_name = 'Nataliia' AND last_name = 'Homon'
  					  LIMIT 1 );
 
COMMIT;

-- Payments for those rentals (match by film title & dates; use film.rental_rate)
--in this case customer who rented film will also pay for it, staff who recieved payment could be other person ffrom this store, that is why I will use table rental and inventory for this purpose
BEGIN transaction;

--example where subqueries are used, gain info about rental details
WITH rent_info as(SELECT rent.rental_id,
					   inven.store_where_rent,
					   rent.customer_id,
					   inven.rental_rate,
				  	   rent.return_date
				FROM public.rental   rent
				INNER JOIN (SELECT customer_id     --find needed customer
							  FROM public.customer
							  WHERE first_name = 'Nataliia' AND last_name = 'Homon'
							  LIMIT 1) AS cust
				ON rent.customer_id = cust.customer_id
				  
				INNER JOIN (SELECT   inv.inventory_id,    --find needed rental info taking into account exact title of film
				  					 inv.store_id AS store_where_rent,
				  					 flm.rental_rate AS rental_rate
				  					 
				  			  FROM public.inventory inv
				  			  INNER JOIN public.film flm ON inv.film_id = flm.film_id
				  			  WHERE flm.title IN (   'Harry Potter and the Philosopher''s Stone',
				    								'Star Wars: Episode IV – A New Hope',
				    								'The Lord of the Rings: The Fellowship of the Ring')
				  			  ) AS inven
				 ON rent.inventory_id = inven.inventory_id
				 /*INNER JOIN ( SELECT distinct stf.staff_id
							  FROM public.staff stf
							  INNER JOIN public.inventory rent ON stf.store_id = rent.store_id
							  LIMIT 1
											 )*/
 ),
staff_recieve_payment AS (SELECT distinct stf.staff_id      --find random staff who recieved payment
								FROM public.staff stf
								INNER JOIN rent_info rni ON stf.store_id = rni.store_where_rent
								LIMIT 1
)
 
 
 INSERT INTO public.payment (customer_id, staff_id, rental_id, amount, payment_date)
 SELECT rni.customer_id,
 		stfp.staff_id,
 		rni.rental_id,
 		rni.rental_rate, --in this case film was returned in time, so amount = rental_rate
 		rni.return_date --is the same as return_date as it was returned in time
 		
 FROM rent_info rni
 CROSS JOIN staff_recieve_payment stfp
 WHERE NOT EXISTS (   --re-runnable
					  SELECT 1
					  FROM public.payment pay
					  WHERE pay.rental_id   = rni.rental_id
					    AND pay.payment_date = rni.return_date
					)
RETURNING payment_id, rental_id, amount, payment_date, staff_id, customer_id;

--doble check table
SELECT *
FROM public.payment pay
WHERE customer_id = (SELECT customer_id     --find needed customer
							  FROM public.customer
							  WHERE first_name = 'Nataliia' AND last_name = 'Homon'
							  LIMIT 1)
 COMMIT;