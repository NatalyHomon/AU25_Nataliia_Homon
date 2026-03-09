-- Create separate database
CREATE DATABASE museum_db;


-- Create schema for our domain
CREATE SCHEMA IF NOT EXISTS museum;

CREATE TABLE IF NOT EXISTS museum.address (
    addr_id INT GENERATED ALWAYS AS IDENTITY,
    addr_country        VARCHAR(100) NOT NULL,
    addr_city           VARCHAR(100) NOT NULL,
    addr_street         VARCHAR(150),
    addr_building_number VARCHAR(20),
    addr_postal_code    VARCHAR(20),
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_address PRIMARY KEY (addr_id)
);

CREATE TABLE IF NOT EXISTS museum.person (
    p_id        INT GENERATED ALWAYS AS IDENTITY,
    p_addr_id   INT,  --FK → address.addr_id
    p_first_name VARCHAR(50) NOT NULL,
    p_last_name  VARCHAR(50) NOT NULL,
    p_birth_date DATE NOT NULL,
    p_email      VARCHAR(50) NOT NULL,
    p_phone      VARCHAR(20) NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_person PRIMARY KEY (p_id),
    CONSTRAINT fk_person_address
        FOREIGN KEY (p_addr_id) REFERENCES museum.address(addr_id),
    CONSTRAINT uq_person_email UNIQUE (p_email)
);

CREATE TABLE IF NOT EXISTS museum.employee (
    em_id         INT GENERATED ALWAYS AS IDENTITY,
    em_person_id  INT NOT NULL,  --FK → person.p_id
    em_position   VARCHAR(100) NOT NULL,
    em_hire_date  DATE NOT NULL,
    em_end_date	  DATE,
    em_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_employee PRIMARY KEY (em_id),
    CONSTRAINT fk_employee_person
        FOREIGN KEY (em_person_id) REFERENCES museum.person(p_id)
);

CREATE TABLE IF NOT EXISTS museum.category (
    cat_id          INT GENERATED ALWAYS AS IDENTITY,
    cat_name        VARCHAR(100) NOT NULL,
    cat_description TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_category PRIMARY KEY (cat_id)
    
   -- CONSTRAINT uq_category_name UNIQUE (cat_name) will be added via alter command
);

CREATE TABLE IF NOT EXISTS museum.storage_location (
    sl_id           INT GENERATED ALWAYS AS IDENTITY,
    sl_name         VARCHAR(100) NOT NULL,
    sl_building     VARCHAR(100) NOT NULL,
    sl_room         VARCHAR(50), --not null constraint will be added via alter
    sl_shelf        VARCHAR(50),
    sl_climate_zone VARCHAR(50),
   
    
    CONSTRAINT pk_storage_location PRIMARY KEY (sl_id)
);

CREATE TABLE IF NOT EXISTS museum.museum_item (
    mi_id              INT GENERATED ALWAYS AS IDENTITY,
    mi_category_id     INT NOT NULL, --FK → category.cat_id
    mi_location_id     INT NOT NULL, --storage_location.sl_id
    mi_inventory_number VARCHAR(50) NOT NULL,
    mi_title           VARCHAR(200) NOT NULL,
    mi_author          VARCHAR(200),
    mi_description     TEXT,
    mi_period          VARCHAR(100),
    mi_acquisition_date DATE,
    mi_estimated_value  NUMERIC(12,2),
    mi_condition_status VARCHAR(50),
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_museum_item PRIMARY KEY (mi_id),
    CONSTRAINT fk_museum_item_category
        FOREIGN KEY (mi_category_id) REFERENCES museum.category(cat_id),
    CONSTRAINT fk_museum_item_location
        FOREIGN KEY (mi_location_id) REFERENCES museum.storage_location(sl_id),
    CONSTRAINT uq_museum_item_inventory UNIQUE (mi_inventory_number)
    
    --check (mi_condition_status IN ('excellent', 'good', 'fair', 'poor', 'damaged', 'restoration_needed', 'lost')) will be added via ALTER
);


CREATE TABLE IF NOT EXISTS museum.exhibition (
    ex_id           INT GENERATED ALWAYS AS IDENTITY,
    ex_title        VARCHAR(200) NOT NULL,
    ex_description  TEXT,
    ex_venue        VARCHAR(200),
    ex_start_date   DATE NOT NULL,
    ex_end_date     DATE,
    ex_is_online    BOOLEAN NOT NULL DEFAULT FALSE,
    ex_online_url   VARCHAR(500),
    ex_status       VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_exhibition PRIMARY KEY (ex_id),
    CONSTRAINT check_ex_status
        CHECK (ex_status IN ('planned', 'active', 'finished'))

);

CREATE TABLE IF NOT EXISTS museum.exhibition_item (
    exi_exhibition_id INT NOT NULL, --FK → exhibition.ex_id
    exi_item_id       INT NOT NULL, --FK → museum_item.mi_id
    exi_role          VARCHAR(50),
    exi_label_text    TEXT NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_exhibition_item
        PRIMARY KEY (exi_exhibition_id, exi_item_id),
    CONSTRAINT fk_exhibition_item_exhibition
        FOREIGN KEY (exi_exhibition_id) REFERENCES museum.exhibition(ex_id),
    CONSTRAINT fk_exhibition_item_item
        FOREIGN KEY (exi_item_id) REFERENCES museum.museum_item(mi_id)
        
        --CHECK (exi_role IN ('main', 'supporting', 'promo', 'interactive' )) in alter command
);

CREATE TABLE IF NOT EXISTS museum.exhibition_curator (
    exc_exhibition_id INT NOT NULL, --FK → exhibition.ex_id
    exc_employee_id   INT NOT NULL, --FK → employee.e_id
    exc_role          VARCHAR(50) NOT NULL,
    exc_from_date     DATE NOT NULL,
    exc_to_date       DATE,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_exhibition_curator
        PRIMARY KEY (exc_exhibition_id, exc_employee_id),

    CONSTRAINT fk_exhibition_curator_exhibition
        FOREIGN KEY (exc_exhibition_id) REFERENCES museum.exhibition(ex_id),
    CONSTRAINT fk_exhibition_curator_employee
        FOREIGN KEY (exc_employee_id) REFERENCES museum.employee(em_id)
        
        --CHECK (exc_role IN  ('chief_curator', 'assistant_curator',  'guest_curator', 'research_curator', 'co_curator')) in alter command
);

CREATE TABLE IF NOT EXISTS museum.visit (
    v_id            INT GENERATED ALWAYS AS IDENTITY,
    v_person_id     INT NOT NULL, --FK → person.p_id 
    v_exhibition_id INT NOT NULL, -- FK → exhibition.ex_id
    v_visit_datetime TIMESTAMP NOT NULL,
    v_ticket_type   VARCHAR(20) NOT NULL,
    v_price         NUMERIC(10,2) NOT NULL,
    v_channel       VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_visit PRIMARY KEY (v_id),
    CONSTRAINT fk_visit_person
        FOREIGN KEY (v_person_id) REFERENCES museum.person(p_id),
    CONSTRAINT fk_visit_exhibition
        FOREIGN KEY (v_exhibition_id) REFERENCES museum.exhibition(ex_id)
        
        --CHECK (v_channel IN ('offline', 'online', 'partner'), CHECK (v_ticket_type IN ('adult', 'child', 'special') will be added via alter
       
);

CREATE TABLE IF NOT EXISTS museum.payment (
    pay_id     INT GENERATED ALWAYS AS IDENTITY,
    pay_visit_id INT NOT NULL,
    pay_amount NUMERIC(10,2) NOT NULL,
    pay_date   TIMESTAMP NOT NULL,
    pay_method VARCHAR(50) NOT NULL,
    pay_status VARCHAR(20) NOT NULL,
    pay_transaction_ref VARCHAR(100) NOT NULL,
    pay_year   INT GENERATED ALWAYS AS (EXTRACT(YEAR FROM pay_date)) STORED,   --was added to present this functionality as requested in the task
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_payment PRIMARY KEY (pay_id),
    CONSTRAINT fk_payment_visit
        FOREIGN KEY (pay_visit_id) REFERENCES museum.visit(v_id),
    CONSTRAINT uq_payment_visit UNIQUE (pay_visit_id)
    
    --CHECK (pay_status IN ('pending',  'paid',  'refunded',  'failed')), CHECK (pay_method IN ('cash', 'card', 'online', 'bank_transfer')) will be added in alter command
);

CREATE TABLE IF NOT EXISTS museum.inventory_check (
    ic_id          INT GENERATED ALWAYS AS IDENTITY,
    ic_location_id INT NOT NULL, --FK → storage_location.sl_id
    ic_employee_id INT NOT NULL,  --FK → employee.e_id
    ic_check_date  DATE NOT NULL,
    ic_comment     TEXT,
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_inventory_check PRIMARY KEY (ic_id),
    CONSTRAINT fk_inventory_check_location
        FOREIGN KEY (ic_location_id) REFERENCES museum.storage_location(sl_id),
    CONSTRAINT fk_inventory_check_employee
        FOREIGN KEY (ic_employee_id) REFERENCES museum.employee(em_id)
);

CREATE TABLE IF NOT EXISTS museum.inventory_check_item (
    ici_check_id INT NOT NULL, --inventory_check.ic_id
    ici_item_id  INT NOT NULL, --museum_item.mi_id
    ici_status   VARCHAR(20) NOT NULL,
    ici_condition TEXT,
    ici_note      TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_inventory_check_item
        PRIMARY KEY (ici_check_id, ici_item_id),

    CONSTRAINT fk_inventory_check_item_check
        FOREIGN KEY (ici_check_id) REFERENCES museum.inventory_check(ic_id),
    CONSTRAINT fk_inventory_check_item_item
        FOREIGN KEY (ici_item_id) REFERENCES museum.museum_item(mi_id)
        
        --CHECK (ici_status IN ('found','missing','moved','damaged')) will be added via alter
);

/*Use ALTER TABLE to add at least 5 check constraints across the tables to restrict certain values, as example 
date to be inserted, which must be greater than January 1, 2024
inserted measured value that cannot be negative
inserted value that can only be a specific value
unique
not null
*/


-- Exhibition start date no earlier than 2024-01-01
ALTER TABLE museum.exhibition
    ADD CONSTRAINT chk_exhibition_start_date
    CHECK (ex_start_date >= DATE '2024-01-01');

-- The estimated value of the exhibit is not negative.
ALTER TABLE museum.museum_item
    ADD CONSTRAINT chk_museum_item_estimated_value_non_negative
    CHECK (mi_estimated_value >= 0);

--Item condition status
ALTER TABLE museum.museum_item
    ADD CONSTRAINT chk_museum_item_condition_status
    CHECK (mi_condition_status IN (
        'excellent',
        'good',
        'fair',
        'poor',
        'damaged',
        'restoration_needed',
        'lost'
    ));

-- Ticket sales channel
ALTER TABLE museum.visit
    ADD CONSTRAINT chk_visit_channel
    CHECK (v_channel IN ('offline', 'online', 'partner'));

-- Ticket type
ALTER TABLE museum.visit
    ADD CONSTRAINT chk_visit_ticket_type
    CHECK (v_ticket_type IN ('adult', 'child', 'special'));

-- The ticket price cannot be negative.
ALTER TABLE museum.visit
    ADD CONSTRAINT chk_visit_price_non_negative
    CHECK (v_price >= 0);

-- Payment status
ALTER TABLE museum.payment
    ADD CONSTRAINT chk_payment_status
    CHECK (pay_status IN ('pending', 'paid', 'refunded', 'failed'));

-- The payment amount cannot be negative.
ALTER TABLE museum.payment
    ADD CONSTRAINT chk_payment_amount_non_negative
    CHECK (pay_amount >= 0);

-- Item status during inventory
ALTER TABLE museum.inventory_check_item
    ADD CONSTRAINT chk_inventory_check_item_status
    CHECK (ici_status IN ('found', 'missing', 'moved', 'damaged'));

-- The role of the exhibit at the exhibition
ALTER TABLE museum.exhibition_item
    ADD CONSTRAINT chk_exhibition_item_role
    CHECK (exi_role IN ('main', 'supporting', 'promo', 'interactive'));

-- The role of the curator
ALTER TABLE museum.exhibition_curator
    ADD CONSTRAINT chk_exhibition_curator_role
    CHECK (exc_role IN (
        'chief_curator',
        'assistant_curator',
        'guest_curator',
        'research_curator',
        'co_curator'
    ));

-- Adding NOT NULL constraint
ALTER TABLE museum.storage_location ALTER COLUMN sl_room SET NOT NULL;

--Adding UNIQUE constraint
ALTER TABLE museum.category ADD CONSTRAINT uq_category_cat_name UNIQUE (cat_name);

--The payment method option
ALTER TABLE museum.payment
    ADD CONSTRAINT chk_payment_method
    CHECK (pay_method IN ('cash', 'card', 'online', 'bank_transfer'));

--The function which will add tgigger on updated_at
CREATE OR REPLACE FUNCTION museum.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Creating triggers on tables where updated_at exists

CREATE TRIGGER trg_person_update_timestamp
BEFORE UPDATE ON museum.person
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_employee_update_timestamp
BEFORE UPDATE ON museum.employee
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_category_update_timestamp
BEFORE UPDATE ON museum.category
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_museum_item_update_timestamp
BEFORE UPDATE ON museum.museum_item
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_exhibition_update_timestamp
BEFORE UPDATE ON museum.exhibition
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_exhibition_item_update_timestamp
BEFORE UPDATE ON museum.exhibition_item
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_exhibition_curator_update_timestamp
BEFORE UPDATE ON museum.exhibition_curator
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_visit_update_timestamp
BEFORE UPDATE ON museum.visit
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_payment_update_timestamp
BEFORE UPDATE ON museum.payment
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_inventory_check_update_timestamp
BEFORE UPDATE ON museum.inventory_check
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

CREATE TRIGGER trg_inventory_check_item_update_timestamp
BEFORE UPDATE ON museum.inventory_check_item
FOR EACH ROW
EXECUTE FUNCTION museum.update_timestamp();

--Inserting data
--The table address
BEGIN TRANSACTION;

WITH new_addr AS (
    SELECT * FROM (VALUES
        ('Ukraine', 'Kyiv',      'Hrushevskoho', '1A', '01001'),
        ('Ukraine', 'Kyiv',      'Sichovykh Striltsiv', '15', '04053'),
        ('Ukraine', 'Lviv',      'Shevchenka',   '25B', '79001'),
        ('Ukraine', 'Odesa',     'Deribasivska', '10',  '65026'),
        ('Ukraine', 'Kharkiv',   'Sumskа',       '50',  '61000'),
        ('Ukraine', 'Dnipro',    'Yavornytskoho','12',  '49000')
    ) v(addr_country, addr_city, addr_street, addr_building_number, addr_postal_code)
)
INSERT INTO museum.address(addr_country, addr_city, addr_street, addr_building_number, addr_postal_code)
SELECT nad.addr_country,
       nad.addr_city,
       nad.addr_street,
       nad.addr_building_number,
       nad.addr_postal_code
FROM new_addr nad
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.address adr
    WHERE adr.addr_country = nad.addr_country
      AND adr.addr_city = nad.addr_city
      AND adr.addr_street = nad.addr_street
      AND adr.addr_building_number = nad.addr_building_number
)
RETURNING addr_id, addr_country, addr_city, addr_street, addr_building_number, addr_postal_code
;

COMMIT;

--The table person
BEGIN TRANSACTION;

WITH new_prs AS (
    SELECT * FROM (VALUES
        ('Anna',   'Pavlik',   DATE '1990-05-10', 'anna.pavlik@example.com',   '+380637000001',
            'Ukraine','Kyiv','Hrushevskoho','1A','01001'),
        ('Olga',   'Ravlik',   DATE '1991-06-15', 'olga.ravlik@example.com',   '+380637000002',
            'Ukraine','Kyiv','Sichovykh Striltsiv','15','04053'),
        ('Petro',  'Ivanov',   DATE '1985-03-20', 'petro.ivanov@example.com',  '+380637000003',
            'Ukraine','Lviv','Shevchenka','25B','79001'),
        ('Iryna',  'Koval',    DATE '1993-11-05', 'iryna.koval@example.com',   '+380637000004',
            'Ukraine','Odesa','Deribasivska','10','65026'),
        ('Maksym', 'Shevchuk', DATE '1988-09-18', 'maksym.shevchuk@example.com','+380637000005',
            'Ukraine','Kharkiv','Sumskа','50','61000'),
        ('Sofia',  'Melnyk',   DATE '1995-02-01', 'sofia.melnyk@example.com',  '+380637000006',
            'Ukraine','Dnipro','Yavornytskoho','12','49000')
    ) v(p_first_name, p_last_name, p_birth_date, p_email, p_phone,
        addr_country, addr_city, addr_street, addr_building_number, addr_postal_code)
)
INSERT INTO museum.person(p_addr_id, p_first_name, p_last_name, p_birth_date, p_email, p_phone)
SELECT adr.addr_id,
       npr.p_first_name,
       npr.p_last_name,
       npr.p_birth_date,
       npr.p_email,
       npr.p_phone
FROM new_prs npr
JOIN museum.address adr
  ON adr.addr_country = npr.addr_country
 AND adr.addr_city = npr.addr_city
 AND adr.addr_street = npr.addr_street
 AND adr.addr_building_number = npr.addr_building_number
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.person prn
    WHERE prn.p_email = npr.p_email
)
RETURNING p_id, p_addr_id, p_first_name, p_last_name, p_birth_date, p_email, p_phone;

COMMIT;

--The table employee
BEGIN TRANSACTION;

WITH new_emp AS (
    SELECT * FROM (VALUES
        ('anna.pavlik@example.com',   'chief curator',        CURRENT_DATE - INTERVAL '80 days'),
        ('olga.ravlik@example.com',   'assistant curator',    CURRENT_DATE - INTERVAL '60 days'),
        ('petro.ivanov@example.com',  'research curator',     CURRENT_DATE - INTERVAL '40 days'),
        ('iryna.koval@example.com',   'exhibition manager',   CURRENT_DATE - INTERVAL '30 days'),
        ('maksym.shevchuk@example.com','collection manager', CURRENT_DATE - INTERVAL '50 days'),
        ('sofia.melnyk@example.com',  'guide',                CURRENT_DATE - INTERVAL '20 days')
    ) v(p_email, em_position, em_hire_date)
)
INSERT INTO museum.employee(em_person_id, em_position, em_hire_date)
SELECT prn.p_id,
       nem.em_position,
       nem.em_hire_date
FROM new_emp nem
JOIN museum.person prn
  ON prn.p_email = nem.p_email
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.employee emp
    WHERE emp.em_person_id = prn.p_id
      AND emp.em_position = nem.em_position
)
RETURNING em_id, em_person_id, em_position, em_hire_date, em_end_date, em_active;

COMMIT;

--the table category
BEGIN TRANSACTION;

WITH new_cat AS (
    SELECT * FROM (VALUES
        ('Painting',      'Oil, acrylic and watercolor paintings'),
        ('Sculpture',     'Stone, metal, wood sculptures'),
        ('Graphics',      'Prints, drawings, etchings'),
        ('Photography',   'Art photography and documentary'),
        ('Textile',       'Tapestries, costumes, fabric art'),
        ('Ceramics',      'Ceramic art and pottery')
    ) v(cat_name, cat_description)
)
INSERT INTO museum.category(cat_name, cat_description)
SELECT nct.cat_name,
       nct.cat_description
FROM new_cat nct
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.category cat
    WHERE cat.cat_name = nct.cat_name
)
RETURNING cat_id, cat_name, cat_description;

COMMIT;

--the table storage_location
BEGIN TRANSACTION;

WITH new_slc AS (
    SELECT * FROM (VALUES
        ('Main Hall A',      'Building A', '101', 'A1', 'standard'),
        ('Main Hall B',      'Building A', '102', 'B1', 'standard'),
        ('Fund Storage C',   'Building B', '201', 'C3', 'cold'),
        ('Fund Storage D',   'Building B', '202', 'D2', 'dry'),
        ('Gallery E',        'Building C', '301', 'E1', 'standard'),
        ('Gallery F',        'Building C', '302', 'F1', 'standard')
    ) v(sl_name, sl_building, sl_room, sl_shelf, sl_climate_zone)
)
INSERT INTO museum.storage_location(sl_name, sl_building, sl_room, sl_shelf, sl_climate_zone)
SELECT nsl.sl_name,
       nsl.sl_building,
       nsl.sl_room,
       nsl.sl_shelf,
       nsl.sl_climate_zone
FROM new_slc nsl
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.storage_location slc
    WHERE slc.sl_name = nsl.sl_name
      AND slc.sl_room = nsl.sl_room
)
RETURNING sl_id, sl_name, sl_building, sl_room, sl_shelf, sl_climate_zone;

COMMIT;

--the table museum_item

BEGIN TRANSACTION;

WITH new_mit AS (
    SELECT * FROM (VALUES
        ('INV-0001', 'Sunset over Dnipro',   'O. Hrytsenko', 'Oil painting',      '20th century',
            DATE '2024-09-10',  5000.00, 'excellent',    'Painting',    'Main Hall A'),
        ('INV-0002', 'Kyiv Landscape',       'I. Marchenko', 'Cityscape sketch', '21st century',
            DATE '2024-08-15',  3200.00, 'good',         'Graphics',    'Main Hall B'),
        ('INV-0003', 'Stone Figure',         'Unknown',     'Granite sculpture','19th century',
            DATE '2024-07-20',  7500.00, 'fair',         'Sculpture',   'Fund Storage C'),
        ('INV-0004', 'Old Tapestry',         'Unknown',     'Medieval textile',  '18th century',
            DATE '2024-10-01', 12000.00, 'restoration_needed','Textile','Fund Storage D'),
        ('INV-0005', 'Porcelain Vase',       'European',    'Decorative vase',   '19th century',
            DATE '2024-09-25',  4100.00, 'good',         'Ceramics',    'Gallery E'),
        ('INV-0006', 'City at Night',        'M. Shevchuk', 'Night photography', '21st century',
            DATE '2024-11-05',  2800.00, 'excellent',    'Photography', 'Gallery F')
    ) v(mi_inventory_number, mi_title, mi_author, mi_description, mi_period,
        mi_acquisition_date, mi_estimated_value, mi_condition_status,
        cat_name, sl_name)
)
INSERT INTO museum.museum_item(
    mi_category_id,
    mi_location_id,
    mi_inventory_number,
    mi_title,
    mi_author,
    mi_description,
    mi_period,
    mi_acquisition_date,
    mi_estimated_value,
    mi_condition_status
)
SELECT cat.cat_id,
       slc.sl_id,
       nmi.mi_inventory_number,
       nmi.mi_title,
       nmi.mi_author,
       nmi.mi_description,
       nmi.mi_period,
       nmi.mi_acquisition_date,
       nmi.mi_estimated_value,
       nmi.mi_condition_status
FROM new_mit nmi
JOIN museum.category cat
  ON cat.cat_name = nmi.cat_name
JOIN museum.storage_location slc
  ON slc.sl_name = nmi.sl_name
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.museum_item mit
    WHERE mit.mi_inventory_number = nmi.mi_inventory_number
)
RETURNING mi_id, mi_category_id, mi_location_id, mi_inventory_number,  mi_title, mi_author, mi_description, mi_period, mi_acquisition_date, mi_estimated_value, mi_condition_status;

COMMIT;

-- the table exhibition

BEGIN TRANSACTION;

WITH new_exh AS (
    SELECT * FROM (VALUES
        ('Dnipro Stories',          'Modern Ukrainian art about Dnipro river',      'Main Hall A',
            CURRENT_DATE - INTERVAL '70 days', CURRENT_DATE - INTERVAL '40 days',
            FALSE, NULL, 'finished'),
        ('Urban Kyiv',              'Photography of Kyiv streets',                  'Gallery F',
            CURRENT_DATE - INTERVAL '50 days', CURRENT_DATE - INTERVAL '20 days',
            TRUE,  'https://museum.example.com/urban-kyiv', 'finished'),
        ('Sculptures in Light',     'Stone and metal sculptures with lighting',     'Main Hall B',
            CURRENT_DATE - INTERVAL '30 days', CURRENT_DATE + INTERVAL '10 days',
            FALSE, NULL, 'active'),
        ('Textile Heritage',        'Tapestries and traditional costumes',          'Gallery E',
            CURRENT_DATE - INTERVAL '20 days', CURRENT_DATE + INTERVAL '40 days',
            TRUE, 'https://museum.example.com/textile', 'active'),
        ('Ceramic Dialogues',       'Contemporary ceramic art',                     'Fund Storage D',
            CURRENT_DATE - INTERVAL '15 days', CURRENT_DATE + INTERVAL '45 days',
            FALSE, NULL, 'active'),
        ('Night City Lights',       'Night photography collection',                 'Gallery F',
            CURRENT_DATE - INTERVAL '10 days', CURRENT_DATE + INTERVAL '60 days',
            TRUE, 'https://museum.example.com/night-city', 'planned')
    ) v(ex_title, ex_description, ex_venue,
        ex_start_date, ex_end_date,
        ex_is_online, ex_online_url, ex_status)
)
INSERT INTO museum.exhibition(ex_title, ex_description, ex_venue, ex_start_date, ex_end_date, ex_is_online, ex_online_url, ex_status)
SELECT nex.ex_title,
       nex.ex_description,
       nex.ex_venue,
       nex.ex_start_date,
       nex.ex_end_date,
       nex.ex_is_online,
       nex.ex_online_url,
       nex.ex_status
FROM new_exh nex
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.exhibition exb
    WHERE exb.ex_title = nex.ex_title
)
RETURNING ex_id,ex_title, ex_description, ex_venue, ex_start_date, ex_end_date, ex_is_online, ex_online_url, ex_status;

COMMIT;

--the table exhibition_curator

BEGIN TRANSACTION;

WITH new_exc AS (
    SELECT * FROM (VALUES
        ('Dnipro Stories',      'anna.pavlik@example.com',   'chief_curator',
            CURRENT_DATE - INTERVAL '75 days', CURRENT_DATE - INTERVAL '40 days'),
        ('Urban Kyiv',          'olga.ravlik@example.com',   'chief_curator',
            CURRENT_DATE - INTERVAL '55 days', CURRENT_DATE - INTERVAL '20 days'),
        ('Sculptures in Light', 'petro.ivanov@example.com',  'chief_curator',
            CURRENT_DATE - INTERVAL '35 days', NULL),
        ('Textile Heritage',    'iryna.koval@example.com',   'chief_curator',
            CURRENT_DATE - INTERVAL '25 days', NULL),
        ('Ceramic Dialogues',   'maksym.shevchuk@example.com','research_curator',
            CURRENT_DATE - INTERVAL '15 days', NULL),
        ('Night City Lights',   'sofia.melnyk@example.com',  'assistant_curator',
            CURRENT_DATE - INTERVAL '10 days', NULL)
    ) v(ex_title, p_email, exc_role, exc_from_date, exc_to_date)
)
INSERT INTO museum.exhibition_curator( exc_exhibition_id, exc_employee_id, exc_role, exc_from_date, exc_to_date)
SELECT exb.ex_id,
       emp.em_id,
       nec.exc_role,
       nec.exc_from_date,
       nec.exc_to_date
FROM new_exc nec
JOIN museum.exhibition exb
  ON exb.ex_title = nec.ex_title
JOIN museum.person prn
  ON prn.p_email = nec.p_email
JOIN museum.employee emp
  ON emp.em_person_id = prn.p_id
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.exhibition_curator exc
    WHERE exc.exc_exhibition_id = exb.ex_id
      AND exc.exc_employee_id = emp.em_id
)
RETURNING exc_exhibition_id, exc_employee_id, exc_role, exc_from_date, exc_to_date;

COMMIT;

--the table exhibition_item
BEGIN TRANSACTION;

WITH new_exi AS (
    SELECT * FROM (VALUES
        ('Dnipro Stories',      'INV-0001', 'main',        'Key painting of the exhibition'),
        ('Dnipro Stories',      'INV-0002', 'supporting',  'Additional city landscape'),
        ('Urban Kyiv',          'INV-0006', 'main',        'Central night photo of Kyiv'),
        ('Sculptures in Light', 'INV-0003', 'main',        'Main stone figure under light'),
        ('Textile Heritage',    'INV-0004', 'main',        'Large historical tapestry'),
        ('Ceramic Dialogues',   'INV-0005', 'main',        'Signature porcelain vase')
    ) v(ex_title, mi_inventory_number, exi_role, exi_label_text)
)
INSERT INTO museum.exhibition_item(exi_exhibition_id, exi_item_id, exi_role, exi_label_text)
SELECT exb.ex_id,
       mit.mi_id,
       nxi.exi_role,
       nxi.exi_label_text
FROM new_exi nxi
JOIN museum.exhibition exb
  ON exb.ex_title = nxi.ex_title
JOIN museum.museum_item mit
  ON mit.mi_inventory_number = nxi.mi_inventory_number
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.exhibition_item exi
    WHERE exi.exi_exhibition_id = exb.ex_id
      AND exi.exi_item_id = mit.mi_id
)
RETURNING exi_exhibition_id, exi_item_id, exi_role, exi_label_text;

COMMIT;

--the table visit
BEGIN TRANSACTION;

WITH new_vst AS (
    SELECT * FROM (VALUES
        ('anna.pavlik@example.com',   'Dnipro Stories',      CURRENT_TIMESTAMP - INTERVAL '65 days', 'adult', 150.00, 'offline'),
        ('olga.ravlik@example.com',   'Urban Kyiv',          CURRENT_TIMESTAMP - INTERVAL '45 days', 'adult', 180.00, 'online'),
        ('petro.ivanov@example.com',  'Sculptures in Light', CURRENT_TIMESTAMP - INTERVAL '25 days', 'adult', 160.00, 'partner'),
        ('iryna.koval@example.com',   'Textile Heritage',    CURRENT_TIMESTAMP - INTERVAL '18 days', 'adult', 170.00, 'offline'),
        ('maksym.shevchuk@example.com','Ceramic Dialogues',  CURRENT_TIMESTAMP - INTERVAL '12 days', 'adult', 155.00, 'online'),
        ('sofia.melnyk@example.com',  'Night City Lights',   CURRENT_TIMESTAMP - INTERVAL '5 days',  'special', 0.00,  'partner')
    ) v(p_email, ex_title, v_visit_datetime, v_ticket_type, v_price, v_channel)
)
INSERT INTO museum.visit(v_person_id, v_exhibition_id, v_visit_datetime, v_ticket_type, v_price, v_channel)
SELECT prn.p_id,
       exb.ex_id,
       nvs.v_visit_datetime,
       nvs.v_ticket_type,
       nvs.v_price,
       nvs.v_channel
FROM new_vst nvs
JOIN museum.person prn
  ON prn.p_email = nvs.p_email
JOIN museum.exhibition exb
  ON exb.ex_title = nvs.ex_title
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.visit vst
    WHERE vst.v_person_id = prn.p_id
      AND vst.v_exhibition_id = exb.ex_id
      AND DATE(vst.v_visit_datetime) = DATE(nvs.v_visit_datetime)
)
RETURNING v_id, v_person_id, v_exhibition_id, v_visit_datetime, v_ticket_type, v_price, v_channel;

COMMIT;

--the table payment
BEGIN TRANSACTION;

WITH new_pay AS (
    SELECT * FROM (VALUES
        ('anna.pavlik@example.com',   'Dnipro Stories',      'cash',          'paid'),
        ('olga.ravlik@example.com',   'Urban Kyiv',          'online',        'paid'),
        ('petro.ivanov@example.com',  'Sculptures in Light', 'card',          'paid'),
        ('iryna.koval@example.com',   'Textile Heritage',    'cash',          'paid'),
        ('maksym.shevchuk@example.com','Ceramic Dialogues',  'online',        'pending'),
        ('sofia.melnyk@example.com',  'Night City Lights',   'online',       'refunded')
    ) v(p_email, ex_title, pay_method, pay_status)
)
INSERT INTO museum.payment(pay_visit_id, pay_amount, pay_date, pay_method, pay_status, pay_transaction_ref)
SELECT vst.v_id,
       vst.v_price,
       CURRENT_TIMESTAMP - INTERVAL '1 days',
       npy.pay_method,
       npy.pay_status,
       'TX-' || vst.v_id::text
FROM new_pay npy
JOIN museum.person prn
  ON prn.p_email = npy.p_email
JOIN museum.exhibition exb
  ON exb.ex_title = npy.ex_title
JOIN museum.visit vst
  ON vst.v_person_id = prn.p_id
 AND vst.v_exhibition_id = exb.ex_id
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.payment pay
    WHERE pay.pay_visit_id = vst.v_id
)
RETURNING pay_id, pay_visit_id, pay_amount, pay_date, pay_method, pay_status, pay_transaction_ref;

COMMIT;

--the table inventory_check
BEGIN TRANSACTION;

WITH new_chk AS (
    SELECT * FROM (VALUES
        ('Fund Storage C',   'maksym.shevchuk@example.com', CURRENT_DATE - INTERVAL '30 days', 'Monthly check of sculptures'),
        ('Fund Storage D',   'maksym.shevchuk@example.com', CURRENT_DATE - INTERVAL '25 days', 'Textile condition review'),
        ('Main Hall A',      'anna.pavlik@example.com',     CURRENT_DATE - INTERVAL '20 days', 'Check paintings before new show'),
        ('Main Hall B',      'petro.ivanov@example.com',    CURRENT_DATE - INTERVAL '15 days', 'Check sculpture lighting safety'),
        ('Gallery E',        'iryna.koval@example.com',     CURRENT_DATE - INTERVAL '10 days', 'Before textile exhibition opening'),
        ('Gallery F',        'sofia.melnyk@example.com',    CURRENT_DATE - INTERVAL '5 days',  'Check photography hanging system')
    ) v(sl_name, p_email, ic_check_date, ic_comment)
)
INSERT INTO museum.inventory_check( ic_location_id, ic_employee_id, ic_check_date, ic_comment)
SELECT slc.sl_id,
       emp.em_id,
       nch.ic_check_date,
       nch.ic_comment
FROM new_chk nch
JOIN museum.storage_location slc
  ON slc.sl_name = nch.sl_name
JOIN museum.person prn
  ON prn.p_email = nch.p_email
JOIN museum.employee emp
  ON emp.em_person_id = prn.p_id
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.inventory_check ich
    WHERE ich.ic_location_id = slc.sl_id
      AND ich.ic_employee_id = emp.em_id
      AND ich.ic_check_date = nch.ic_check_date
)
RETURNING ic_id, ic_location_id, ic_employee_id, ic_check_date, ic_comment;

COMMIT;

--the table inventory_check_item
BEGIN TRANSACTION;

WITH new_ici AS (
    SELECT * FROM (VALUES
        ('Fund Storage C',   CURRENT_DATE - INTERVAL '30 days', 'INV-0003', 'found',   'No visible damage',          NULL),
        ('Fund Storage D',   CURRENT_DATE - INTERVAL '25 days', 'INV-0004', 'damaged', 'Tears on lower part',        'Needs restoration'),
        ('Main Hall A',      CURRENT_DATE - INTERVAL '20 days', 'INV-0001', 'found',   'Stable condition',           NULL),
        ('Main Hall B',      CURRENT_DATE - INTERVAL '15 days', 'INV-0002', 'found',   'Frame slightly scratched',   NULL),
        ('Gallery E',        CURRENT_DATE - INTERVAL '10 days', 'INV-0005', 'found',   'No changes detected',        NULL),
        ('Gallery F',        CURRENT_DATE - INTERVAL '5 days',  'INV-0006', 'moved',   'Moved to another wall',      'Position updated in plan')
    ) v(sl_name, ic_check_date, mi_inventory_number, ici_status, ici_condition, ici_note)
)
INSERT INTO museum.inventory_check_item(ici_check_id, ici_item_id, ici_status, ici_condition, ici_note)
SELECT ich.ic_id,
       mit.mi_id,
       nic.ici_status,
       nic.ici_condition,
       nic.ici_note
FROM new_ici nic
JOIN museum.storage_location slc
  ON slc.sl_name = nic.sl_name
JOIN museum.inventory_check ich
  ON ich.ic_location_id = slc.sl_id
 AND ich.ic_check_date = nic.ic_check_date
JOIN museum.museum_item mit
  ON mit.mi_inventory_number = nic.mi_inventory_number
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.inventory_check_item ici
    WHERE ici.ici_check_id = ich.ic_id
      AND ici.ici_item_id = mit.mi_id
)
RETURNING ici_check_id, ici_item_id, ici_status, ici_condition, ici_note;

COMMIT;

/*5.1 Create a function that updates data in one of your tables. This function should take the following input arguments:
The primary key value of the row you want to update
The name of the column you want to update
The new value you want to set for the specified column

This function should be designed to modify the specified row in the table, updating the specified column with the new value.
*/

/*Used dynamic SQL as it allows you to build SQL queries as text and execute them while the program is running.
 *  In regular SQL, you cannot change the structure of the query through parameters. PostgreSQL does not know that "column_name" is a column name.*/
CREATE OR REPLACE FUNCTION museum.update_data_person(
    p_id_to_update INT,
    p_column_name  TEXT,
    p_new_value    TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    -- List of allowed columns that can be updated
    allowed_columns CONSTANT TEXT[] := ARRAY[
        'p_first_name',
        'p_last_name',
        'p_birth_date',
        'p_email',
        'p_phone',
        'p_addr_id'
    ];

    column_type TEXT;  -- Will store the data type of the target column
    sql_text    TEXT;  -- Will store dynamically generated SQL query
BEGIN
    -- Validate the ID input
    IF p_id_to_update IS NULL THEN
        RAISE EXCEPTION 'ID value cannot be NULL';
    END IF;

    -- Validate the column name input
    IF p_column_name IS NULL OR p_column_name = '' THEN
        RAISE EXCEPTION 'Column name cannot be NULL or empty';
    END IF;

    -- Allow updates only to a predefined set of safe columns
    IF NOT (p_column_name = ANY(allowed_columns)) THEN
        RAISE EXCEPTION 'Column "%" is not allowed to be updated', p_column_name;
    END IF;

    -- Validate that the new value is not empty 
    IF p_new_value IS NULL OR p_new_value = '' THEN
        RAISE EXCEPTION 'New value cannot be NULL or empty';
    END IF;

    -- Check if the record with the given ID exists
    IF NOT EXISTS (SELECT 1 FROM museum.person p WHERE p.p_id = p_id_to_update) THEN
        RAISE EXCEPTION 'Person with id % does not exist', p_id_to_update;
    END IF;

    -- Determine the data type of the target column from the system catalog
    SELECT cols.data_type
    INTO column_type
    FROM information_schema.columns cols
    WHERE cols.table_schema = 'museum'
      AND cols.table_name   = 'person'
      AND cols.column_name  = p_column_name;

    IF column_type IS NULL THEN
        RAISE EXCEPTION 'Column "%" not found in table "museum.person"', p_column_name;
    END IF;

    --  Build a dynamic SQL query that casts the input value into the correct data type
    sql_text := format(
        'UPDATE museum.person SET %I = %L::%s WHERE p_id = %s',
        p_column_name,
        p_new_value,
        column_type,
        p_id_to_update
    );

    --  Execute the dynamically generated SQL statement
    EXECUTE sql_text;

    --  Informative message after successful update
    RAISE NOTICE 'Row with id % successfully updated: % = % (cast to type %)',
        p_id_to_update, p_column_name, p_new_value, column_type;

END;
$$;

--check
SELECT *
FROM museum.person per
WHERE per.p_id = 1;
--p_first_name => 'Anna'

SELECT museum.update_data_person(1, 'p_first_name', 'Olga');

SELECT *
FROM museum.person per
WHERE per.p_id = 1;
--p_first_name => 'Olga'



/*5. 2 Create a function that adds a new transaction to your transaction table. 
You can define the input arguments and output format. 
Make sure all transaction attributes can be set with the function (via their natural keys). 
The function does not need to return a value but should confirm the successful insertion of the new transaction.
*/

--first will create new visit without payment, as in this database logic 1 visit has 1 payment
BEGIN TRANSACTION;

WITH new_vst AS (
    SELECT * FROM (VALUES
       				  ('olga.ravlik@example.com',   'Textile Heritage',    CURRENT_TIMESTAMP, 'adult', 170.00, 'offline')
       				) v(p_email, ex_title, v_visit_datetime, v_ticket_type, v_price, v_channel)
)
INSERT INTO museum.visit(v_person_id, v_exhibition_id, v_visit_datetime, v_ticket_type, v_price, v_channel)
SELECT prn.p_id,
       exb.ex_id,
       nvs.v_visit_datetime,
       nvs.v_ticket_type,
       nvs.v_price,
       nvs.v_channel
FROM new_vst nvs
JOIN museum.person prn
  ON prn.p_email = nvs.p_email
JOIN museum.exhibition exb
  ON exb.ex_title = nvs.ex_title
WHERE NOT EXISTS (
    SELECT 1
    FROM museum.visit vst
    WHERE vst.v_person_id = prn.p_id
      AND vst.v_exhibition_id = exb.ex_id
      AND DATE(vst.v_visit_datetime) = DATE(nvs.v_visit_datetime)
)
RETURNING v_id, v_person_id, v_exhibition_id, v_visit_datetime, v_ticket_type, v_price, v_channel;

COMMIT;
SELECT * FROM museum.visit;

--creat function
CREATE OR REPLACE FUNCTION museum.add_payment_transaction(
    p_person_email      TEXT,        -- natural key for person
    p_exhibition_title  TEXT,        -- natural key for exhibition
    p_visit_datetime    TIMESTAMP,   -- natural key part for visit
    p_pay_amount        NUMERIC,     -- transaction amount
    p_pay_method        TEXT,        -- e.g. 'cash', 'card', 'online', 'bank_transfer'
    p_pay_status        TEXT,        -- e.g. 'pending', 'paid', 'refunded', 'failed'
    p_transaction_ref   TEXT,        -- optional external reference, can be NULL
    p_pay_date          TIMESTAMP    -- optional, can be NULL, will default to now()
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE 
	v_person_id_new      INT;
    v_exhibition_id_new  INT;
    v_visit_id_new       INT;
    v_pay_date_new       TIMESTAMP;
    v_transaction_ref_new TEXT;
BEGIN
	--validation for required logical keys

	IF p_person_email IS NULL OR p_person_email = '' THEN
        RAISE EXCEPTION 'Person email must be provided';
    END IF;

    IF p_exhibition_title IS NULL OR p_exhibition_title = '' THEN
        RAISE EXCEPTION 'Exhibition title must be provided';
    END IF;

    IF p_visit_datetime IS NULL THEN
        RAISE EXCEPTION 'Visit datetime must be provided';
    END IF;
    
    --find person id
    SELECT p_id
    INTO v_person_id_new
    FROM museum.person
    WHERE p_email = p_person_email;

    IF v_person_id_new IS NULL THEN
        RAISE EXCEPTION 'Person with email "%" does not exist', p_person_email;
    END IF;
    
    --find exhibition by natural key (title)
    SELECT ex_id
    INTO v_exhibition_id_new
    FROM museum.exhibition
    WHERE ex_title = p_exhibition_title;

    IF v_exhibition_id_new IS NULL THEN
        RAISE EXCEPTION 'Exhibition with title "%" does not exist', p_exhibition_title;
    END IF;
    
    --find visit by natural keys (person + exhibition + visit datetime)
    SELECT vis.v_id
    INTO v_visit_id_new
    FROM museum.visit vis
    WHERE vis.v_person_id     = v_person_id_new
      AND vis.v_exhibition_id = v_exhibition_id_new
      AND DATE(vis.v_visit_datetime) = DATE(p_visit_datetime);

    IF v_visit_id_new IS NULL THEN
        RAISE EXCEPTION
            'Visit not found for person "%", exhibition "%", datetime %',
            p_person_email, p_exhibition_title, p_visit_datetime;
    END IF;
    
    --Check if a payment for this visit already exists
    IF EXISTS (
        SELECT 1 FROM museum.payment pay
        WHERE pay.pay_visit_id = v_visit_id_new
    ) THEN
        RAISE EXCEPTION
            'Payment for visit id % already exists', v_visit_id_new;
    END IF;

    --resolve pay_date (use current timestamp if not provided)
    v_pay_date_new := COALESCE(p_pay_date, CURRENT_TIMESTAMP);
    
    --resolve transaction reference (generate if empty or NULL)
    IF p_transaction_ref IS NULL OR p_transaction_ref = '' THEN
        v_transaction_ref_new := format(
            'TX-%s',
            v_visit_id_new
        );
    ELSE
        v_transaction_ref_new := p_transaction_ref;
    END IF;
    
    --checks for amount and method/status
    
    IF p_pay_amount IS NULL OR p_pay_amount < 0 THEN
        RAISE EXCEPTION 'Payment amount must be non-negative';
    END IF;

    IF p_pay_method IS NULL OR p_pay_method = '' THEN
        RAISE EXCEPTION 'Payment method must be provided';
    END IF;

    IF p_pay_status IS NULL OR p_pay_status = '' THEN
        RAISE EXCEPTION 'Payment status must be provided';
    END IF;
    
    --insert the new payment transaction
    INSERT INTO museum.payment(
        pay_visit_id,
        pay_amount,
        pay_date,
        pay_method,
        pay_status,
        pay_transaction_ref
    )
    VALUES (
        v_visit_id_new,
        p_pay_amount,
        v_pay_date_new,
        p_pay_method,
        p_pay_status,
        v_transaction_ref_new
    );
    
    --confirmation message
    RAISE NOTICE
        'Payment transaction successfully created: visit_id=%, amount=%, method=%, status=%, transaction_ref=%',
        v_visit_id_new, p_pay_amount, p_pay_method, p_pay_status, v_transaction_ref_new;

END;
$$;

SELECT museum.add_payment_transaction('olga.ravlik@example.com', 'Textile Heritage', CURRENT_TIMESTAMP::timestamp, 170.00, 'card', 'paid',  'TX-9', TIMESTAMP '2025-11-17 14:32:31.926'); 

SELECT* FROM museum.payment;

/*Create a view that presents analytics for the most recently added quarter in your database. Ensure that the result excludes irrelevant fields such as surrogate keys and duplicate entries.
*/

CREATE OR REPLACE VIEW museum.v_quarterly_exhibition_analytics AS
WITH latest_quarter AS (
    -- determine the most recent quarter based on payment dates
    SELECT date_trunc('quarter', MAX(pay_date)) AS quarter_start
    FROM museum.payment
),
payments_in_quarter AS (
    -- select all payments that belong to the most recent quarter
    SELECT pay.pay_visit_id,
           pay.pay_amount,
           pay.pay_status,
           pay.pay_date,
           lq.quarter_start
    FROM museum.payment pay
    CROSS JOIN latest_quarter lq
    WHERE date_trunc('quarter', pay.pay_date) = lq.quarter_start
)
SELECT
    piq.quarter_start::date AS quarter_start_date,
    (piq.quarter_start + INTERVAL '3 months - 1 day')::date AS quarter_end_date,
    exb.ex_title AS exhibition_title,
    COUNT(DISTINCT vst.v_id) AS total_visits,
    COUNT(DISTINCT CASE WHEN piq.pay_status = 'paid'
                        THEN vst.v_id END) AS paid_visits,
    COUNT(DISTINCT per.p_id) AS unique_visitors,
    SUM(CASE WHEN piq.pay_status = 'paid'
             THEN piq.pay_amount
             ELSE 0 END) AS total_revenue,
    AVG(CASE WHEN piq.pay_status = 'paid'
             THEN piq.pay_amount
        END) AS avg_ticket_revenue,
    COUNT(DISTINCT CASE WHEN vst.v_channel = 'online'
                        THEN vst.v_id END) AS online_visits,
    COUNT(DISTINCT CASE WHEN vst.v_channel = 'offline'
                        THEN vst.v_id END) AS offline_visits
FROM payments_in_quarter piq
JOIN museum.visit vst ON vst.v_id = piq.pay_visit_id
JOIN museum.exhibition exb ON exb.ex_id = vst.v_exhibition_id
JOIN museum.person per ON per.p_id = vst.v_person_id
GROUP BY piq.quarter_start, exb.ex_title;

SELECT * FROM museum.v_quarterly_exhibition_analytics;

