CREATE SCHEMA IF NOT EXISTS  data_quality;

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER file_server
FOREIGN DATA WRAPPER file_fdw;

CREATE FOREIGN TABLE data_quality.bank (
    "age" INT,
    job TEXT,
    marital TEXT,
    education TEXT,
    "default" TEXT,
    balance INT,
    housing TEXT,
    loan TEXT,
    contact TEXT,
    duration INT
)
SERVER file_server
OPTIONS (
    filename 'C:/src_data/DQ_Task3/bank.csv',
    format 'csv',
    header 'true'
);

SELECT * FROM data_quality.bank LIMIT 10;

--Missing values NULL
SELECT
    COUNT(*) FILTER (WHERE "age" IS NULL) AS age_missing,
    COUNT(*) FILTER (WHERE job IS NULL) AS job_missing,
    COUNT(*) FILTER (WHERE marital IS NULL) AS marital_missing,
    COUNT(*) FILTER (WHERE education IS NULL) AS education_missing,
    COUNT(*) FILTER (WHERE "default" IS NULL) AS default_missing,
    COUNT(*) FILTER (WHERE balance IS NULL) AS balance_missing,
    COUNT(*) FILTER (WHERE housing IS NULL) AS housing_missing,
    COUNT(*) FILTER (WHERE loan IS NULL) AS loan_missing,
    COUNT(*) FILTER (WHERE contact IS NULL) AS contact_missing,
    COUNT(*) FILTER (WHERE duration IS NULL) AS duration_missing
FROM data_quality.bank;

--Missing values 'unknown' + NULL
SELECT 'age' AS column_name, COUNT(*) 
FROM data_quality.bank WHERE age IS NULL

UNION ALL

SELECT 'job', COUNT(*) 
FROM data_quality.bank WHERE job IS NULL OR job = 'unknown'

UNION ALL

SELECT 'marital', COUNT(*) 
FROM data_quality.bank WHERE marital IS NULL OR marital = 'unknown'

UNION ALL

SELECT 'education', COUNT(*) 
FROM data_quality.bank WHERE education IS NULL OR education = 'unknown'

UNION ALL

SELECT 'default', COUNT(*) 
FROM data_quality.bank WHERE "default" IS NULL OR "default" = 'unknown'

UNION ALL

SELECT 'balance', COUNT(*) 
FROM data_quality.bank WHERE balance IS NULL

UNION ALL

SELECT 'housing', COUNT(*) 
FROM data_quality.bank WHERE housing IS NULL OR housing = 'unknown'

UNION ALL

SELECT 'loan', COUNT(*) 
FROM data_quality.bank WHERE loan IS NULL OR loan = 'unknown'

UNION ALL

SELECT 'contact', COUNT(*) 
FROM data_quality.bank WHERE contact IS NULL OR contact = 'unknown'

UNION ALL

SELECT 'duration', COUNT(*) 
FROM data_quality.bank WHERE duration IS NULL;

--Invalid values +imbalance

SELECT job, COUNT(*) 
FROM data_quality.bank
GROUP BY job
ORDER BY COUNT(*) DESC;

SELECT marital, COUNT(*) 
FROM data_quality.bank
GROUP BY marital
ORDER BY COUNT(*) DESC;

SELECT education, COUNT(*) 
FROM data_quality.bank
GROUP BY education
ORDER BY COUNT(*) DESC;

SELECT "default", COUNT(*) 
FROM data_quality.bank
GROUP BY "default"
ORDER BY COUNT(*) DESC;

SELECT housing, COUNT(*) 
FROM data_quality.bank
GROUP BY housing
ORDER BY COUNT(*) DESC;

SELECT loan, COUNT(*) 
FROM data_quality.bank
GROUP BY loan
ORDER BY COUNT(*) DESC;

SELECT contact, COUNT(*) 
FROM data_quality.bank
GROUP BY contact
ORDER BY COUNT(*) DESC;

--metrics 
SELECT 
    MIN(balance), 
    MAX(balance), 
    AVG(balance)
FROM data_quality.bank;

SELECT *
FROM data_quality.bank
WHERE balance < 0;

--dublicates
SELECT *
FROM data_quality.bank
WHERE (age, job, marital, education, "default", balance, housing, loan, contact, duration) IN (
    SELECT age, job, marital, education, "default", balance, housing, loan, contact, duration
    FROM data_quality.bank
    GROUP BY age, job, marital, education, "default", balance, housing, loan, contact, duration
    HAVING COUNT(*) > 1
);

--fuzzy duplicates
SELECT *
FROM data_quality.bank b
WHERE (age, job, marital, education, "default", housing, loan, contact) IN (
    SELECT
        age, job, marital, education, "default", housing, loan, contact
    FROM data_quality.bank
    GROUP BY
        age, job, marital, education, "default", housing, loan, contact
    HAVING COUNT(*) > 1
)
ORDER BY age, job, marital, education;

SELECT *
FROM data_quality.bank
WHERE age < 18;

SELECT *
FROM data_quality.bank
WHERE age > 100;

SELECT *
FROM data_quality.bank
WHERE age <= 0;

SELECT age, COUNT(*)
FROM data_quality.bank
GROUP BY age
ORDER BY age;

SELECT *
FROM data_quality.bank
WHERE duration <= 0
   OR duration < 5
   OR duration > 5000;

