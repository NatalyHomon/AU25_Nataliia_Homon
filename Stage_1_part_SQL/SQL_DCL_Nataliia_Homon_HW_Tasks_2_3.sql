
--Task 2. Implement role-based authentication model for dvd_rental database
--1/Create a new user with the username "rentaluser" and the password "rentalpassword". Give the user the ability to connect to the database but no other permissions.
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'rentaluser'
    ) THEN
        CREATE ROLE rentaluser LOGIN PASSWORD 'rentalpassword';
    END IF;
END
$$;


--check
SELECT *
FROM pg_roles
WHERE rolname = 'rentaluser';

GRANT CONNECT ON DATABASE dvdrental TO rentaluser;


--Grant "rentaluser" SELECT permission for the "customer" table. Сheck to make sure this permission works correctly—write a SQL query to select all customers.

GRANT USAGE ON SCHEMA public TO rentaluser;
GRANT SELECT ON TABLE public.customer TO rentaluser;

--Now check — under the rentaluser role:
SET ROLE rentaluser; 

SELECT *
FROM public.customer;

--3.Create a new user group called "rental" and add "rentaluser" to the group. 

SELECT session_user, current_user;--can check our current state;

RESET ROLE; --go back to superuser functional;

DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'rental'
    ) THEN
        CREATE ROLE rental;
    END IF;
END
$$;
 ---- NOLOGIN by default

SELECT *
FROM pg_roles
WHERE rolname = 'rental';

GRANT rental TO rentaluser;

--check 
SELECT
    r.rolname          AS role_name,
    m.rolname          AS member_name
FROM pg_auth_members am
JOIN pg_roles r ON am.roleid = r.oid
JOIN pg_roles m ON am.member = m.oid
WHERE r.rolname ='rental'
ORDER BY role_name, member_name;

--4.Grant the "rental" group INSERT and UPDATE permissions for the "rental" table. Insert a new row and update one existing row in the "rental" table under that role. 
GRANT USAGE ON SCHEMA public TO rental;
GRANT SELECT, INSERT, UPDATE ON TABLE public.rental TO rental;
GRANT USAGE, UPDATE ON SEQUENCE public.rental_rental_id_seq TO rental;

--check
SELECT table_schema, table_name, grantee, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'rental'
ORDER BY table_schema, table_name, grantee;

SET ROLE rental;

INSERT INTO public.rental (rental_date, inventory_id, customer_id, staff_id)
SELECT current_date, 389, 149, 1
WHERE NOT EXISTS (SELECT 1 
				  FROM public.rental ren
				  WHERE ren.rental_date = current_date
				  AND ren.inventory_id = 389)
RETURNING rental_id;
--check
SELECT *
FROM public.rental ren
WHERE ren.customer_id = 149 
ORDER BY ren.rental_date desc;

--staff_id updating from 1 to 2
UPDATE public.rental 
SET staff_id = 2
WHERE rental_id = 2;

--check
SELECT * 
FROM public.rental
WHERE rental_id = 2;

--5.Revoke the "rental" group's INSERT permission for the "rental" table. Try to insert new rows into the "rental" table make sure this action is denied.
RESET ROLE;
REVOKE INSERT ON public.rental FROM rental; 

--check
SET ROLE rental;

INSERT INTO public.rental (rental_date, inventory_id, customer_id, staff_id)
SELECT current_date, 389, 148, 2
WHERE NOT EXISTS (SELECT 1 
				  FROM public.rental ren
				  WHERE ren.rental_date = current_date
				  AND ren.inventory_id = 389)
RETURNING rental_id;

--6.Create a personalized role for any customer already existing in the dvd_rental database. The name of the role name must be client_{first_name}_{last_name} (omit curly brackets). The customer's payment and rental history must not be empty. 

RESET ROLE;

SELECT cust.customer_id,
       cust.first_name,
       cust.last_name,
       COUNT(DISTINCT ren.rental_id)  AS rentals_cnt,
       COUNT(DISTINCT pay.payment_id) AS payments_cnt
FROM public.customer cust
JOIN public.rental  ren ON ren.customer_id = cust.customer_id
JOIN public.payment pay ON pay.customer_id = cust.customer_id
GROUP BY cust.customer_id, cust.first_name, cust.last_name
HAVING COUNT(DISTINCT ren.rental_id)  > 0
   AND COUNT(DISTINCT pay.payment_id) > 0
LIMIT 1;

--first_name = 'MARY' second_name = 'SMITH', customer_id =1
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'client_mary_smith'
    ) THEN
        CREATE ROLE client_mary_smith
            LOGIN
            PASSWORD 'mary_secret_password';
    END IF;
END
$$;


--check
SELECT *
FROM pg_roles
WHERE rolname = 'client_mary_smith';

SELECT table_schema, table_name, grantee, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'rental';

--Task 3. Implement row-level security
--Configure that role so that the customer can only access their own data in the "rental" and "payment" tables. Write a query to make sure this user sees only their own data.

GRANT CONNECT ON DATABASE dvdrental TO client_mary_smith;
GRANT USAGE ON SCHEMA public TO client_mary_smith;
GRANT SELECT ON TABLE public.rental TO client_mary_smith;
GRANT SELECT ON TABLE public.payment TO client_mary_smith;

--check
SELECT table_schema, table_name, grantee, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'client_mary_smith';

--turn on RLS
ALTER TABLE public.rental ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payment ENABLE ROW LEVEL SECURITY;

--creat policy

--to make universal approach and to avoid givving permission on SELECT on table customer, decided to use function
/*at first used current_user in the func and checked through set role=> function returned null, as physically the user of the session was superuser =>
 changed current_user to session_user and physically loged into database as user client_mary_smith => RLS worked correctly, user client_mary_smith can see only her rentals and payments in her session
 during investagating the issue, one off possible reasons of error was also function search path, that why manually added path for  function get_customer_id_for_current_user()*/

CREATE OR REPLACE FUNCTION get_customer_id_for_current_user()
RETURNS int
LANGUAGE sql
SECURITY DEFINER
AS $$
    SELECT customer_id
    FROM public.customer
    WHERE lower('client_' || first_name || '_' || last_name) = session_user;
$$;

ALTER FUNCTION get_customer_id_for_current_user()
    SET search_path = public;

CREATE POLICY rental_policy_clients
    ON public.rental
    FOR SELECT
    USING (customer_id = get_customer_id_for_current_user());

CREATE POLICY payment_policy_clients
    ON public.payment
    FOR SELECT
    USING (customer_id = get_customer_id_for_current_user());


--investigating tools, left for learning process
/*
SELECT --checked pass
    proname,
    proconfig
FROM pg_proc
WHERE proname = 'get_customer_id_for_current_user';
*/

--call function returned null in this session
--SELECT get_customer_id_for_current_user();

--fuction which helped to find that even if we work in the session like other user, postgree see current_user as user of the session
/*
CREATE OR REPLACE FUNCTION debug_who_am_i()
RETURNS text
LANGUAGE sql
SECURITY DEFINER
AS $$
    SELECT current_user;
$$;

SELECT debug_who_am_i();
SELECT session_user, current_user;
*/



--check
SELECT
    schemaname,
    tablename,
    policyname,
    permissive,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies
ORDER BY schemaname, tablename, policyname;


SET ROLE client_mary_smith;

--check will return empty table, should log in the other session as client_mary_smith

SELECT rental_id, rental_date, customer_id
FROM public.rental
ORDER BY rental_id;

SELECT payment_id, amount, payment_date, customer_id
FROM public.payment
ORDER BY payment_id;