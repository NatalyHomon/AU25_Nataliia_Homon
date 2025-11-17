--create database and schema
CREATE DATABASE auction_house;

CREATE SCHEMA IF NOT EXISTS auction; --create our container-schema, add check if not exists to avoid errors

SET search_path TO auction;  --this was  added to automatically work with schema auction(so we can now not mention schema name in our queries)

--create table member
CREATE TABLE IF NOT EXISTS auction.member(
m_id 				INT GENERATED ALWAYS AS IDENTITY, --PK
m_mail 				VARCHAR(50) NOT NULL,             
m_phone 			VARCHAR(13) NOT NULL, 
m_type 				VARCHAR(10) NOT NULL,
m_registration_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT pk_member PRIMARY KEY (m_id),
CONSTRAINT uq_member_mail UNIQUE (m_mail),  --required info during reqistration
CONSTRAINT uq_member_phone UNIQUE (m_phone),  --required info during reqistration
CONSTRAINT chk_member_type CHECK (m_type IN ('company', 'person'))  --identify whom is registated member(only two options)
);

--create table member_person
CREATE TABLE IF NOT EXISTS auction.member_person(
mp_id 			INT GENERATED ALWAYS AS IDENTITY,  --PK
mp_code 		INT NOT NULL,  --FK → member.m_id
mp_firstname 	VARCHAR(20) NOT NULL,
mp_surname 		VARCHAR(20) NOT NULL,
mp_datebirth 	DATE NOT NULL,

CONSTRAINT pk_member_person PRIMARY KEY (mp_id),
CONSTRAINT fk_member_person_code FOREIGN KEY (mp_code) REFERENCES auction.member(m_id),
CONSTRAINT chk_member_person_datebirth CHECK (mp_datebirth <= CURRENT_DATE - INTERVAL '18 years')  --person should be at least 18 years old - should be adult
);

--create table member_company
CREATE TABLE IF NOT EXISTS auction.member_company(
mc_id 					INT GENERATED ALWAYS AS IDENTITY, --PK
mc_code 				INT NOT NULL,					--FK → member.m_id
mc_legal_name 			VARCHAR(100) NOT NULL,
mc_trade_name 			VARCHAR(100),
mc_registration_number 	VARCHAR(50) NOT NULL,
mc_iso_cod 				VARCHAR(5) 	NOT NULL,
mc_estab_date 			DATE NOT NULL,

CONSTRAINT pk_member_company PRIMARY KEY (mc_id),
CONSTRAINT fk_member_company_code FOREIGN KEY (mc_code) REFERENCES auction.member(m_id),
CONSTRAINT uq_member_company_identifier UNIQUE (mc_legal_name, mc_iso_cod, mc_registration_number),		-- as 2 companies may have the same name, combination with registration number will provide unique value
CONSTRAINT chk_member_company_estab_date CHECK (mc_estab_date >= DATE '2000-01-01')  --such ts was required in the task
);

--create table member_address
CREATE TABLE IF NOT EXISTS auction.member_address (
ma_id        INT GENERATED ALWAYS AS IDENTITY,  --PK
ma_code		 INT NOT NULL,             --FK → member.m_id
ma_country   VARCHAR(50) NOT NULL,
ma_city      VARCHAR(100) NOT NULL,
ma_street    VARCHAR(200) NOT NULL,
ma_zip       VARCHAR(20),

CONSTRAINT pk_member_address PRIMARY KEY (ma_id),
CONSTRAINT fk_member_address_member FOREIGN KEY (ma_code) REFERENCES auction.member (m_id)
);

----create table item
CREATE TABLE IF NOT EXISTS auction.item (
i_id           INT GENERATED ALWAYS AS IDENTITY,  --PK
i_title        VARCHAR(200) NOT NULL,
i_count  	   SMALLINT NOT NULL,
i_description  TEXT NOT NULL,
i_prod_date    DATE NOT NULL,			--product creation date
i_condition	   VARCHAR(10) NOT NULL,

CONSTRAINT pk_item PRIMARY KEY (i_id),
CONSTRAINT chk_item_condition CHECK (i_condition IN ('new', 'used', 'refurbished')), --identify what condition an item has
CONSTRAINT chk_item_count CHECK (i_count >= 0)
);

--create table item_seller
CREATE TABLE IF NOT EXISTS auction.item_seller (
is_item_code     INT       NOT NULL, --PFK → item.i_id
is_member_code   INT       NOT NULL,  --PFK → member.m_id
is_share_percent SMALLINT  NOT NULL,
is_main_member   BOOLEAN   NOT NULL,

CONSTRAINT pk_item_seller PRIMARY KEY (is_item_code, is_member_code), -- composite PK
CONSTRAINT fk_item_seller_item FOREIGN KEY (is_item_code) REFERENCES auction.item (i_id),
CONSTRAINT fk_item_seller_member FOREIGN KEY (is_member_code) REFERENCES auction.member (m_id),
CONSTRAINT chk_item_seller_share_percent CHECK (is_share_percent BETWEEN 0 AND 100)
);

--create table lot_status
CREATE TABLE IF NOT EXISTS auction.lot_status (
ls_id    INT GENERATED ALWAYS AS IDENTITY, --PK
ls_value VARCHAR(20) NOT NULL,

CONSTRAINT pk_lot_status PRIMARY KEY (ls_id),
CONSTRAINT uq_lot_status_value UNIQUE (ls_value)
);

--create table lot_category
CREATE TABLE IF NOT EXISTS auction.lot_category (
lc_id    INT GENERATED ALWAYS AS IDENTITY, --PK
lc_name  VARCHAR(100) NOT NULL,

CONSTRAINT pk_lot_category PRIMARY KEY (lc_id),
CONSTRAINT uq_lot_category_name UNIQUE (lc_name)
);

--create table auction
CREATE TABLE IF NOT EXISTS auction.auction (
au_id         	INT GENERATED ALWAYS AS IDENTITY, --PK
au_title      	VARCHAR(200) NOT NULL,
au_location   	VARCHAR(200) NOT NULL,
au_start_time 	TIMESTAMP NOT NULL,
au_end_time   	TIMESTAMP NOT NULL,
au_description	VARCHAR(20) NOT NULL, 
au_status 		VARCHAR(20) NOT NULL,			-- 'planned','active','closed','cancelled'

CONSTRAINT pk_auction PRIMARY KEY (au_id),
CONSTRAINT chk_auction_dates_order CHECK (au_end_time > au_start_time),
CONSTRAINT chk_auction_status CHECK (au_status IN ('planned','active','closed','cancelled'))
);

--create table ordrers
CREATE TABLE IF NOT EXISTS auction.orders (
o_id             INT GENERATED ALWAYS AS IDENTITY, --PK
o_buyer          INT NOT NULL,   --FK → member.m_id
o_date           TIMESTAMP NOT NULL,
o_status         VARCHAR(50) NOT NULL DEFAULT 'pending',
o_total_amount   NUMERIC(12,2) NOT NULL,
o_number_of_lots SMALLINT NOT NULL,
o_created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
o_updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT pk_order PRIMARY KEY (o_id),
CONSTRAINT fk_order_buyer FOREIGN KEY (o_buyer) REFERENCES auction.member(m_id),
CONSTRAINT chk_order_status CHECK (o_status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
CONSTRAINT chk_order_number_of_lots CHECK (o_number_of_lots >= 1)
);

--create table lot
CREATE TABLE IF NOT EXISTS auction.lot (
l_id            INT GENERATED ALWAYS AS IDENTITY, --PK
l_item_code     INT NOT NULL,        -- FK → item.i_id
l_title         VARCHAR(100) NOT NULL,
l_start_price   NUMERIC(12,2) NOT NULL,
l_final_price   NUMERIC(12,2),
l_lot_number    INT NOT NULL,        -- lot number in the auction
l_auction_id    INT NOT NULL,        -- FK → auction.au_id
l_status        INT NOT NULL,        -- FK → lot_status.ls_id
l_category_id   INT NOT NULL,        -- FK → lot_category.lc_id
l_order_id      INT,                 -- FK → "order".o_id (if was sold)
l_sold_date     TIMESTAMP,
l_created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
l_updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT pk_lot PRIMARY KEY (l_id),
CONSTRAINT fk_lot_item FOREIGN KEY (l_item_code) REFERENCES auction.item (i_id),
CONSTRAINT fk_lot_auction FOREIGN KEY (l_auction_id) REFERENCES auction.auction (au_id),
CONSTRAINT fk_lot_status FOREIGN KEY (l_status) REFERENCES auction.lot_status (ls_id), 
CONSTRAINT fk_lot_category FOREIGN KEY (l_category_id) REFERENCES auction.lot_category (lc_id), 
CONSTRAINT fk_lot_order FOREIGN KEY (l_order_id) REFERENCES auction.orders(o_id),

CONSTRAINT uq_lot_title UNIQUE (l_title),
CONSTRAINT uq_lot_auction_lot_number UNIQUE (l_auction_id, l_lot_number),  -- the lot number within one auction must be unique
CONSTRAINT chk_lot_start_price_non_negative CHECK (l_start_price >= 0) --check price is not negative
);


--create table bid
CREATE TABLE IF NOT EXISTS auction.bid (
b_id          INT GENERATED ALWAYS AS IDENTITY, --PK
b_lot_code    INT NOT NULL,              -- FK → lot.l_id
b_member_code INT NOT NULL,              -- FK → member.m_id (хто ставить ставку)
b_amount      NUMERIC(12,2) NOT NULL,
b_time        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT pk_bid PRIMARY KEY (b_id),
CONSTRAINT fk_bid_lot FOREIGN KEY (b_lot_code) REFERENCES auction.lot (l_id), 
CONSTRAINT fk_bid_member FOREIGN KEY (b_member_code) REFERENCES auction.member (m_id),
CONSTRAINT chk_bid_amount_non_negative CHECK (b_amount >= 0)
);

--create table auction_member
CREATE TABLE IF NOT EXISTS auction.auction_members (
am_auction_code   INT NOT NULL,  -- FK → auction.au_id
am_member_code    INT NOT NULL,  -- FK → member.m_id
am_registration_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT pk_auction_members PRIMARY KEY (am_auction_code, am_member_code), -- composite PK
CONSTRAINT fk_auction_members_auction FOREIGN KEY (am_auction_code) REFERENCES auction.auction (au_id), 
CONSTRAINT fk_auction_members_member FOREIGN KEY (am_member_code) REFERENCES auction.member (m_id)
);

--create table payment_details

CREATE TABLE IF NOT EXISTS auction.payment_details (
pd_id             INT GENERATED ALWAYS AS IDENTITY, -- PK
pd_order_code     INT NOT NULL,						 -- FK → orders.o_id
pd_payment_date   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
pd_amount_paid    NUMERIC(12,2) NOT NULL,
pd_method         VARCHAR(30) NOT NULL,  		--'card','bank_transfer','cash','paypal', etc.
pd_status         VARCHAR(20) NOT NULL DEFAULT 'initiated',   
pd_transaction_id VARCHAR(50),

CONSTRAINT pk_payment_details PRIMARY KEY (pd_id),
CONSTRAINT fk_payment_details_order FOREIGN KEY (pd_order_code) REFERENCES auction.orders (o_id),
CONSTRAINT uq_payment_transaction UNIQUE (pd_transaction_id),  --Unique transaction identifier
CONSTRAINT chk_payment_date_not_before_2000 CHECK (pd_payment_date >= TIMESTAMP '2000-01-01 00:00:00'), -- Payment date no earlier than 01.01.2000 (task requirements)
CONSTRAINT chk_pd_amount_non_negative CHECK (pd_amount_paid >= 0), -- The amount cannot be negative (requirement "measured value cannot be negative"
CONSTRAINT chk_pd_method_allowed CHECK (pd_method IN ('credit_card','bank_transfer','paypal','cash')), -- Allowed payment methods
CONSTRAINT chk_pd_status_allowed CHECK (pd_status IN ('initiated','completed','failed','refunded')) -- Allowed payment statuses
);

--insert values into table member
BEGIN TRANSACTION;
WITH new_member AS (
					SELECT 'person1mail@gmail.com' AS mail,
							'063777777' AS phone,
							'person' AS member_type
					UNION ALL
					SELECT 'person2mail@gmail.com' AS mail,
							'063777778' AS phone,
							'person' AS member_type
					UNION ALL
					SELECT 'company1mail@gmail.com' AS mail,
							'063777779' AS phone,
							'company' AS member_type
					UNION ALL
					SELECT 'company2mail@gmail.com' AS mail,
							'063777770' AS phone,
							'company' AS member_type
)
INSERT INTO auction.member(m_mail, m_phone, m_type)
SELECT nwm.mail,
	   nwm.phone,
	   nwm.member_type
FROM new_member nwm
WHERE NOT EXISTS (	SELECT 1
					FROM auction.member mem
					WHERE mem.m_mail = nwm.mail
					AND mem.m_phone = nwm.phone)
RETURNING m_id, m_mail, m_phone, m_type, m_registration_date;

COMMIT;
 
--insert values into table member_person
BEGIN TRANSACTION; 

WITH new_member_person as(SELECT * FROM (VALUES ('Anna', 'Pavlik', '1990-05-10'::date, 'person1mail@gmail.com'),
												('Olga', 'Ravlik', '1991-05-10'::date, 'person2mail@gmail.com')
										)v(firstname, surname, datebirth, mail)
)
INSERT INTO auction.member_person(mp_code, mp_firstname, mp_surname, mp_datebirth)
SELECT exm.m_id,
	   nwmp.firstname,
	   nwmp.surname,
	   nwmp.datebirth
FROM new_member_person nwmp
INNER JOIN auction.member exm ON nwmp.mail = exm.m_mail
WHERE NOT EXISTS (	SELECT 1
					FROM auction.member_person mep
					WHERE mep.mp_code = exm.m_id
					)
RETURNING mp_code, mp_firstname, mp_surname, mp_datebirth;

COMMIT;

--insert values into table member_company
BEGIN TRANSACTION; 

WITH new_member_company as(SELECT * FROM (VALUES ('Global Auctions LLC', 'Global Auctions', 'REG-1001', 'UA', '2010-01-01'::date, 'company1mail@gmail.com'),
												('Antique Trade Inc.', 'Antique Trade', 'REG-2002', 'UA', '2010-01-01'::date, 'company2mail@gmail.com')
										)v(legal_name, trade_name, registrnum, iso, estab_date, mail)
)
INSERT INTO auction.member_company(mc_code, mc_legal_name, mc_trade_name, mc_registration_number, mc_iso_cod, mc_estab_date)
SELECT exm.m_id,
	   nwmc.legal_name,
	   nwmc.trade_name,
	   nwmc.registrnum,
	   nwmc.iso,
	   nwmc.estab_date
FROM new_member_company nwmc
INNER JOIN auction.member exm ON nwmc.mail = exm.m_mail
WHERE NOT EXISTS (	SELECT 1
					FROM auction.member_company mec
					WHERE mec.mc_code = exm.m_id
					)
RETURNING mc_code, mc_legal_name, mc_trade_name, mc_registration_number, mc_iso_cod, mc_estab_date;

COMMIT;

--adding address
BEGIN TRANSACTION;
WITH address as(SELECT * FROM (VALUES ('Ukraine', 'Kyiv', 'Khreshchatyk 1', '01001', 'person1mail@gmail.com'),
									  ('Ukraine', 'Lviv', 'Svobody Ave 10', '79000', 'person2mail@gmail.com'),
									  ('Ukraine', 'Vinnytsia', 'Svobody Ave 10', '79700', 'company1mail@gmail.com'),
									  ('Ukraine', 'Lviv', 'Svobody Ave 15', '79000', 'company2mail@gmail.com'))
								v(country, city, street, zip, mail)	
)								
INSERT INTO auction.member_address (ma_code, ma_country, ma_city, ma_street, ma_zip)
SELECT exm.m_id, 
	   addr.country,
	   addr.city,
	   addr.street,
	   addr.zip
FROM address addr
INNER JOIN auction.member exm ON addr.mail = exm.m_mail
WHERE NOT EXISTS (	SELECT 1
					FROM auction.member_address addr2
					WHERE addr2.ma_code = exm.m_id
					)
RETURNING ma_id, ma_code, ma_country, ma_city, ma_street, ma_zip;

COMMIT;

--adding items
BEGIN TRANSACTION;

WITH items_new AS (SELECT * FROM (VALUES ('Vintage Vase', 1, 'Porcelain vintage vase.', DATE '2010-03-15', 'used'),
										 ('Antique Watch', 1, 'Gold-plated antique watch.', DATE '2005-06-10', 'used')							
								)v (title, item_count, description, prod_date, item_condition )
)
INSERT INTO auction.item (i_title, i_count, i_description, i_prod_date, i_condition)
SELECT itm.title,
	   itm.item_count,
	   itm.description,
	   itm.prod_date,
	   itm.item_condition
FROM items_new itm
ON CONFLICT DO NOTHING 
RETURNING i_id, i_title, i_count, i_description, i_prod_date, i_condition;

COMMIT;

--bridge table, relation many to many table item_seller
BEGIN TRANSACTION;

WITH new_item_seller AS (
					    SELECT * FROM (VALUES
					        ('Vintage Vase'       , 'person1mail@gmail.com'   , 70            , TRUE),
					        ('Vintage Vase'       , 'person2mail@gmail.com'   , 30            , FALSE),
					        ('Antique Watch'      , 'company1mail@gmail.com'   , 100           , TRUE)
					    ) v(item_title, member_mail, share_percent, is_main_member)
)
INSERT INTO auction.item_seller (is_item_code, is_member_code, is_share_percent, is_main_member)
SELECT
    itm.i_id,
    mem.m_id,
    nis.share_percent,
    nis.is_main_member
FROM new_item_seller nis
JOIN auction.item   itm ON itm.i_title = nis.item_title
JOIN auction.member mem ON mem.m_mail  = nis.member_mail
ON CONFLICT (is_item_code, is_member_code) DO NOTHING
RETURNING is_item_code, is_member_code, is_share_percent, is_main_member;

COMMIT;

-- insert into lot_status
BEGIN TRANSACTION;

WITH new_lot_status AS (
					    SELECT * FROM (VALUES 
					        ('draft'),
					        ('active'),
					        ('sold'),
					        ('cancelled')
					    ) v(status_value)
)
INSERT INTO auction.lot_status (ls_value)
SELECT nls.status_value
FROM new_lot_status nls
ON CONFLICT (ls_value) DO NOTHING  --can use  on conflict as ls_value is UNQ
RETURNING ls_id, ls_value;

COMMIT;

-- insert into lot_category
BEGIN TRANSACTION;

WITH new_lot_category AS (
						    SELECT * FROM (VALUES 
						        ('Antiques'),
						        ('Collectibles'),
						        ('Electronics'),
						        ('Art')
						    ) v(category_name)
)
INSERT INTO auction.lot_category (lc_name)
SELECT nlc.category_name
FROM new_lot_category nlc
ON CONFLICT (lc_name) DO NOTHING --can use  on conflict as lc_name is UNQ
RETURNING lc_id, lc_name;

COMMIT;

--insert into table auction
BEGIN TRANSACTION;

WITH new_auction AS (
					    SELECT * FROM (VALUES
					        ('Spring Art Auction',
					         'Kyiv',
					         TIMESTAMP '2025-03-01 18:00:00',
					         TIMESTAMP '2025-03-01 21:00:00',
					         'evening art lots',
					         'planned'),
					
					        ('Vintage Car Auction',
					         'Lviv',
					         TIMESTAMP '2025-04-15 10:00:00',
					         TIMESTAMP '2025-04-15 16:00:00',
					         'classic cars sale',
					         'planned')
					    ) v(au_title, au_location, au_start_time, au_end_time, au_description, au_status)
)
INSERT INTO auction.auction (au_title, au_location, au_start_time, au_end_time, au_description, au_status)
SELECT  naw.au_title,
        naw.au_location,
        naw.au_start_time,
        naw.au_end_time,
        naw.au_description,
        naw.au_status
FROM new_auction naw
WHERE NOT EXISTS (
    SELECT 1
    FROM auction.auction exa
    WHERE exa.au_title      = naw.au_title
      AND exa.au_start_time = naw.au_start_time
      AND exa.au_location   = naw.au_location
)
RETURNING au_id, au_title, au_location, au_start_time, au_status;

COMMIT;


--insert into table orders
BEGIN TRANSACTION;

WITH new_orders AS (
				    SELECT *
				    FROM (VALUES 
				        ('person2mail@gmail.com',   TIMESTAMP '2025-03-15 20:00:00', 'pending',   150.00::NUMERIC(12,2), 1),
				        ('company2mail@gmail.com',   TIMESTAMP '2025-04-15 16:00:00', 'paid',      300.00::NUMERIC(12,2), 1)
				    ) v(buyer_mail, order_date, order_status, total_amount, number_of_lots)
)
INSERT INTO auction.orders (o_buyer, o_date, o_status, o_total_amount, o_number_of_lots)
SELECT  exm.m_id,
        nwo.order_date,
        nwo.order_status,
        nwo.total_amount,
        nwo.number_of_lots       
FROM new_orders nwo
INNER JOIN auction.member exm 
        ON exm.m_mail = nwo.buyer_mail
WHERE NOT EXISTS (
    SELECT 1
    FROM auction.orders exo
    WHERE exo.o_buyer        = exm.m_id
      AND exo.o_date         = nwo.order_date
      AND exo.o_total_amount = nwo.total_amount
)
RETURNING o_id, o_buyer, o_date, o_status, o_total_amount, o_number_of_lots;

COMMIT;

--insert into  table lot
BEGIN TRANSACTION;
WITH order_info AS (SELECT  ord.o_id AS id,
							mem.m_mail AS mail
					FROM auction.orders ord
					INNER JOIN auction.member mem ON ord.o_buyer = mem.m_id
					WHERE mem.m_mail IN ('person2mail@gmail.com', 'company2mail@gmail.com')
),
new_lots AS (
				    SELECT * FROM (VALUES 
				        ('Vintage Vase Lot',  120.00, 150.00,  1, 'Vintage Vase', 'Spring Art Auction', 'person2mail@gmail.com', TIMESTAMP '2025-03-15 19:00:00' ),
				        ('Antique Watch Lot', 250.00, 300.00,  1, 'Antique Watch', 'Vintage Car Auction', 'company2mail@gmail.com', TIMESTAMP '2025-04-15 15:00:00')
				    ) v (lot_title, start_price, final_price, lot_number, item_title, auction_title, byuer_mail, sold_time)
)
INSERT INTO auction.lot(l_item_code, l_title, l_start_price, l_final_price, l_lot_number, l_auction_id, l_status, l_category_id, l_order_id, l_sold_date)
SELECT  
        exi.i_id,
        nlt.lot_title,
        nlt.start_price,
        nlt.final_price,
        nlt.lot_number,
        exa.au_id,
        exs.ls_id,
        exc.lc_id,
        ord.id,
        nlt.sold_time
FROM new_lots nlt
INNER JOIN auction.item         exi ON exi.i_title = nlt.item_title
INNER JOIN auction.auction      exa ON exa.au_title = nlt.auction_title
INNER JOIN auction.lot_status   exs ON exs.ls_value = 'active'
INNER JOIN auction.lot_category exc ON exc.lc_name  = 'Antiques'
INNER JOIN order_info 			ord ON ord.mail = nlt.byuer_mail
WHERE NOT EXISTS (
      SELECT 1
      FROM auction.lot exl
      WHERE exl.l_auction_id = exa.au_id
        AND exl.l_lot_number = nlt.lot_number
)
RETURNING l_id, l_item_code, l_title, l_start_price, l_final_price, l_lot_number, l_auction_id, l_status, l_category_id, l_order_id, l_sold_date;

COMMIT;

--insert into bid
BEGIN TRANSACTION;

WITH new_bids AS (
				    SELECT * FROM (VALUES
				        ('Vintage Vase Lot', 'person2mail@gmail.com', 150.00),        
				        ('Antique Watch Lot', 'company1mail@gmail.com', 250.50),
				        ('Antique Watch Lot', 'person2mail@gmail.com', 300.00)			        
				    ) v (lot_title, member_mail, amount)
)
INSERT INTO auction.bid (b_lot_code, b_member_code, b_amount)
SELECT
      lot.l_id,
      mbr.m_id,
      nbs.amount
FROM new_bids nbs
JOIN auction.member mbr ON nbs.member_mail = mbr.m_mail
JOIN auction.lot lot ON lot.l_title = nbs.lot_title
WHERE NOT EXISTS (
        SELECT 1 
        FROM auction.bid exb
        WHERE exb.b_lot_code    = lot.l_id
          AND exb.b_member_code = mbr.m_id
          AND exb.b_amount      = nbs.amount
)
RETURNING b_id, b_lot_code, b_member_code, b_amount;

COMMIT;

--insert into auction_members
BEGIN TRANSACTION;

WITH new_auction_members AS (
    SELECT * FROM (VALUES 
        ('Spring Art Auction', 'person1mail@gmail.com'),
        ('Vintage Car Auction', 'person1mail@gmail.com'),
        ('Spring Art Auction', 'person2mail@gmail.com'),
        ('Vintage Car Auction', 'person2mail@gmail.com'),
        ('Spring Art Auction', 'company1mail@gmail.com'),
        ('Vintage Car Auction', 'company1mail@gmail.com'),
        ('Spring Art Auction', 'company2mail@gmail.com'),
        ('Vintage Car Auction', 'company2mail@gmail.com')
        ) v (auction_title, member_mail)
)
INSERT INTO auction.auction_members (am_auction_code, am_member_code, am_registration_date)
SELECT 
       exa.au_id,
       exm.m_id,
       CURRENT_TIMESTAMP
FROM new_auction_members naw
INNER JOIN auction.auction exa ON naw.auction_title = exa.au_title
INNER JOIN auction.member exm ON naw.member_mail = exm.m_mail
WHERE NOT EXISTS (
        SELECT 1
        FROM auction.auction_members exmbr
        WHERE exmbr.am_auction_code = exa.au_id
          AND exmbr.am_member_code  = exm.m_id
      )
RETURNING am_auction_code, am_member_code, am_registration_date;

COMMIT;

--insert into table payment.details
BEGIN TRANSACTION;

WITH new_payment_details AS (
						    SELECT *
						    FROM (VALUES 
						            ('2025-03-15 20:00:00.000'::timestamp,  150.00, 'credit_card',  'initiated', 'TXN-001'),
						            ('2025-04-15 16:00:00.000'::timestamp,  300.00, 'paypal',       'completed', 'TXN-002')
						         ) v(pd_payment_date, pd_amount_paid, pd_method, pd_status, pd_transaction_id)
),

resolved_orders AS (
				    SELECT
				        ord.o_id,
				        ord.o_date
				    FROM auction.orders ord
				    WHERE ord.o_date IN (
				        '2025-03-15 20:00:00.000'::timestamp,
				        '2025-04-15 16:00:00.000'::timestamp
				    )
),

prepared_rows AS (
				    SELECT
				        ro.o_id AS pd_order_code,
				        npd.pd_payment_date,
				        npd.pd_amount_paid,
				        npd.pd_method,
				        npd.pd_status,
				        npd.pd_transaction_id
				    FROM new_payment_details npd
				    INNER JOIN resolved_orders ro ON ro.o_date = npd.pd_payment_date
)

INSERT INTO auction.payment_details(pd_order_code, pd_payment_date, pd_amount_paid, pd_method, pd_status, pd_transaction_id)
SELECT 
    pr.pd_order_code,
    pr.pd_payment_date,
    pr.pd_amount_paid,
    pr.pd_method,
    pr.pd_status,
    pr.pd_transaction_id
FROM prepared_rows pr
WHERE NOT EXISTS (
    SELECT 1
    FROM auction.payment_details exd
    WHERE exd.pd_transaction_id = pr.pd_transaction_id
)
RETURNING 
    pd_id, pd_order_code, pd_payment_date, pd_amount_paid, pd_method, pd_status, pd_transaction_id;

COMMIT;
SELECT * FROM auction.orders
--Add a not null 'record_ts' field to each table using ALTER TABLE statements, set the default value to current_date, and check to make sure the value has been set for the existing rows.

ALTER TABLE auction.member
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.member_person
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.member_company
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.member_address
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.item
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.item_seller
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.lot_status
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.lot_category
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.auction
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.orders
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.lot
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.bid
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.auction_members
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

ALTER TABLE auction.payment_details
    ADD COLUMN IF NOT EXISTS record_ts DATE NOT NULL DEFAULT CURRENT_DATE;

--checking result
SELECT 'member'            AS table_name, COUNT(*) FROM auction.member            WHERE record_ts IS NULL
UNION ALL SELECT 'member_person',   COUNT(*) FROM auction.member_person   WHERE record_ts IS NULL
UNION ALL SELECT 'member_company',  COUNT(*) FROM auction.member_company  WHERE record_ts IS NULL
UNION ALL SELECT 'member_address',  COUNT(*) FROM auction.member_address  WHERE record_ts IS NULL
UNION ALL SELECT 'item',            COUNT(*) FROM auction.item            WHERE record_ts IS NULL
UNION ALL SELECT 'item_seller',     COUNT(*) FROM auction.item_seller     WHERE record_ts IS NULL
UNION ALL SELECT 'lot_status',      COUNT(*) FROM auction.lot_status      WHERE record_ts IS NULL
UNION ALL SELECT 'lot_category',    COUNT(*) FROM auction.lot_category    WHERE record_ts IS NULL
UNION ALL SELECT 'auction',         COUNT(*) FROM auction.auction         WHERE record_ts IS NULL
UNION ALL SELECT 'orders',          COUNT(*) FROM auction.orders          WHERE record_ts IS NULL
UNION ALL SELECT 'lot',             COUNT(*) FROM auction.lot             WHERE record_ts IS NULL
UNION ALL SELECT 'bid',             COUNT(*) FROM auction.bid             WHERE record_ts IS NULL
UNION ALL SELECT 'auction_members', COUNT(*) FROM auction.auction_members WHERE record_ts IS NULL
UNION ALL SELECT 'payment_details', COUNT(*) FROM auction.payment_details WHERE record_ts IS NULL;
