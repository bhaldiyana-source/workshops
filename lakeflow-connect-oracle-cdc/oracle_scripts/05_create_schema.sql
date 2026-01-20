-- ===================================================================
-- Script: 05_create_schema.sql
-- Purpose: Create RETAIL schema and tables for CDC workshop
-- Requirements: DBA privileges
-- Downtime: No
-- ===================================================================

-- Connect as DBA
-- sqlplus system/password@RETAILDB

-- ===================================================================
-- STEP 1: Create RETAIL schema user
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Creating RETAIL schema user...
PROMPT ===================================================================

-- Drop user if exists (for clean workshop setup)
-- Uncomment if you need to start fresh:
-- DROP USER RETAIL CASCADE;

CREATE USER RETAIL IDENTIFIED BY "RetailSchema123!"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

PROMPT RETAIL user created.

-- ===================================================================
-- STEP 2: Grant necessary privileges to RETAIL user
-- ===================================================================
PROMPT
PROMPT Granting privileges to RETAIL user...

GRANT CONNECT TO RETAIL;
GRANT RESOURCE TO RETAIL;
GRANT CREATE TABLE TO RETAIL;
GRANT CREATE VIEW TO RETAIL;
GRANT CREATE SEQUENCE TO RETAIL;
GRANT CREATE PROCEDURE TO RETAIL;

PROMPT Privileges granted.

-- ===================================================================
-- STEP 3: Connect as RETAIL user to create schema objects
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Creating RETAIL schema tables...
PROMPT ===================================================================

-- Note: In actual execution, you would connect as RETAIL user:
-- CONNECT RETAIL/RetailSchema123!@RETAILDB

-- For this script, we'll create as DBA with schema prefix

-- ===================================================================
-- TABLE 1: CUSTOMERS (Parent table)
-- ===================================================================
PROMPT
PROMPT Creating CUSTOMERS table...

CREATE TABLE RETAIL.CUSTOMERS (
    CUSTOMER_ID NUMBER(10) NOT NULL,
    FIRST_NAME VARCHAR2(50) NOT NULL,
    LAST_NAME VARCHAR2(50) NOT NULL,
    EMAIL VARCHAR2(100) NOT NULL,
    PHONE VARCHAR2(20),
    ADDRESS VARCHAR2(200),
    CITY VARCHAR2(50),
    STATE VARCHAR2(2),
    ZIP_CODE VARCHAR2(10),
    CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_CUSTOMERS PRIMARY KEY (CUSTOMER_ID),
    CONSTRAINT UK_CUSTOMER_EMAIL UNIQUE (EMAIL)
);

-- Add comments for documentation
COMMENT ON TABLE RETAIL.CUSTOMERS IS 'Customer master data - Parent table for orders';
COMMENT ON COLUMN RETAIL.CUSTOMERS.CUSTOMER_ID IS 'Unique customer identifier (primary key)';
COMMENT ON COLUMN RETAIL.CUSTOMERS.EMAIL IS 'Customer email address (unique)';
COMMENT ON COLUMN RETAIL.CUSTOMERS.CREATED_DATE IS 'Record creation timestamp';
COMMENT ON COLUMN RETAIL.CUSTOMERS.LAST_UPDATED IS 'Last modification timestamp';

-- Create indexes for performance
CREATE INDEX RETAIL.IDX_CUSTOMERS_EMAIL ON RETAIL.CUSTOMERS(EMAIL);
CREATE INDEX RETAIL.IDX_CUSTOMERS_STATE ON RETAIL.CUSTOMERS(STATE);
CREATE INDEX RETAIL.IDX_CUSTOMERS_CITY ON RETAIL.CUSTOMERS(CITY);
CREATE INDEX RETAIL.IDX_CUSTOMERS_LAST_UPDATED ON RETAIL.CUSTOMERS(LAST_UPDATED);

PROMPT CUSTOMERS table created with indexes.

-- ===================================================================
-- TABLE 2: PRODUCTS (Independent table)
-- ===================================================================
PROMPT
PROMPT Creating PRODUCTS table...

CREATE TABLE RETAIL.PRODUCTS (
    PRODUCT_ID NUMBER(10) NOT NULL,
    PRODUCT_NAME VARCHAR2(100) NOT NULL,
    CATEGORY VARCHAR2(50),
    PRICE NUMBER(10, 2) NOT NULL,
    STOCK_QUANTITY NUMBER(10) DEFAULT 0 NOT NULL,
    SUPPLIER VARCHAR2(100),
    DESCRIPTION CLOB,
    IS_ACTIVE CHAR(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_PRODUCTS PRIMARY KEY (PRODUCT_ID),
    CONSTRAINT CHK_PRICE CHECK (PRICE >= 0),
    CONSTRAINT CHK_STOCK CHECK (STOCK_QUANTITY >= 0),
    CONSTRAINT CHK_ACTIVE CHECK (IS_ACTIVE IN ('Y', 'N'))
);

-- Add comments
COMMENT ON TABLE RETAIL.PRODUCTS IS 'Product catalog with inventory levels';
COMMENT ON COLUMN RETAIL.PRODUCTS.PRODUCT_ID IS 'Unique product identifier (primary key)';
COMMENT ON COLUMN RETAIL.PRODUCTS.PRICE IS 'Product price in USD';
COMMENT ON COLUMN RETAIL.PRODUCTS.STOCK_QUANTITY IS 'Current inventory level';
COMMENT ON COLUMN RETAIL.PRODUCTS.IS_ACTIVE IS 'Product availability flag (Y/N)';

-- Create indexes
CREATE INDEX RETAIL.IDX_PRODUCTS_CATEGORY ON RETAIL.PRODUCTS(CATEGORY);
CREATE INDEX RETAIL.IDX_PRODUCTS_NAME ON RETAIL.PRODUCTS(PRODUCT_NAME);
CREATE INDEX RETAIL.IDX_PRODUCTS_PRICE ON RETAIL.PRODUCTS(PRICE);
CREATE INDEX RETAIL.IDX_PRODUCTS_ACTIVE ON RETAIL.PRODUCTS(IS_ACTIVE);

PROMPT PRODUCTS table created with indexes.

-- ===================================================================
-- TABLE 3: ORDERS (Child table with FK to CUSTOMERS)
-- ===================================================================
PROMPT
PROMPT Creating ORDERS table...

CREATE TABLE RETAIL.ORDERS (
    ORDER_ID NUMBER(10) NOT NULL,
    CUSTOMER_ID NUMBER(10) NOT NULL,
    ORDER_DATE TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    ORDER_STATUS VARCHAR2(20) DEFAULT 'PENDING' NOT NULL,
    TOTAL_AMOUNT NUMBER(10, 2) NOT NULL,
    SHIPPING_ADDRESS VARCHAR2(200),
    PAYMENT_METHOD VARCHAR2(50),
    CREATED_DATE TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) 
        REFERENCES RETAIL.CUSTOMERS(CUSTOMER_ID) ON DELETE CASCADE,
    CONSTRAINT CHK_ORDER_AMOUNT CHECK (TOTAL_AMOUNT >= 0),
    CONSTRAINT CHK_ORDER_STATUS CHECK (ORDER_STATUS IN (
        'PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED'
    ))
);

-- Add comments
COMMENT ON TABLE RETAIL.ORDERS IS 'Order transactions - Child table referencing customers';
COMMENT ON COLUMN RETAIL.ORDERS.ORDER_ID IS 'Unique order identifier (primary key)';
COMMENT ON COLUMN RETAIL.ORDERS.CUSTOMER_ID IS 'Foreign key to CUSTOMERS table';
COMMENT ON COLUMN RETAIL.ORDERS.ORDER_STATUS IS 'Order lifecycle status';
COMMENT ON COLUMN RETAIL.ORDERS.TOTAL_AMOUNT IS 'Order total in USD';

-- Create indexes
CREATE INDEX RETAIL.IDX_ORDERS_CUSTOMER ON RETAIL.ORDERS(CUSTOMER_ID);
CREATE INDEX RETAIL.IDX_ORDERS_DATE ON RETAIL.ORDERS(ORDER_DATE);
CREATE INDEX RETAIL.IDX_ORDERS_STATUS ON RETAIL.ORDERS(ORDER_STATUS);
CREATE INDEX RETAIL.IDX_ORDERS_LAST_UPDATED ON RETAIL.ORDERS(LAST_UPDATED);

PROMPT ORDERS table created with indexes and foreign key constraint.

-- ===================================================================
-- STEP 4: Enable supplemental logging on tables (required for CDC)
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Enabling table-level supplemental logging for CDC...
PROMPT ===================================================================

ALTER TABLE RETAIL.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
PROMPT Supplemental logging enabled on CUSTOMERS

ALTER TABLE RETAIL.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
PROMPT Supplemental logging enabled on ORDERS

ALTER TABLE RETAIL.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
PROMPT Supplemental logging enabled on PRODUCTS

-- ===================================================================
-- STEP 5: Create sequences (optional, for auto-incrementing IDs)
-- ===================================================================
PROMPT
PROMPT Creating sequences for ID generation...

CREATE SEQUENCE RETAIL.SEQ_CUSTOMER_ID
    START WITH 1000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE SEQUENCE RETAIL.SEQ_ORDER_ID
    START WITH 10000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

CREATE SEQUENCE RETAIL.SEQ_PRODUCT_ID
    START WITH 100
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

PROMPT Sequences created.

-- ===================================================================
-- STEP 6: Create helper views (optional)
-- ===================================================================
PROMPT
PROMPT Creating helper views...

-- View: Customer with order count
CREATE OR REPLACE VIEW RETAIL.V_CUSTOMER_ORDERS AS
SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.CITY,
    c.STATE,
    COUNT(o.ORDER_ID) AS ORDER_COUNT,
    SUM(o.TOTAL_AMOUNT) AS TOTAL_SPENT,
    MAX(o.ORDER_DATE) AS LAST_ORDER_DATE
FROM RETAIL.CUSTOMERS c
LEFT JOIN RETAIL.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.CITY, c.STATE;

PROMPT Helper views created.

-- ===================================================================
-- STEP 7: Gather statistics
-- ===================================================================
PROMPT
PROMPT Gathering table statistics...

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'CUSTOMERS');
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'ORDERS');
    DBMS_STATS.GATHER_TABLE_STATS('RETAIL', 'PRODUCTS');
END;
/

PROMPT Statistics gathered.

-- ===================================================================
-- STEP 8: Verify schema creation
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT Verifying schema creation...
PROMPT ===================================================================

SELECT 
    TABLE_NAME,
    NUM_ROWS,
    LAST_ANALYZED
FROM DBA_TABLES
WHERE OWNER = 'RETAIL'
ORDER BY TABLE_NAME;

PROMPT
PROMPT Constraints:

SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    CONSTRAINT_TYPE,
    STATUS
FROM DBA_CONSTRAINTS
WHERE OWNER = 'RETAIL'
  AND CONSTRAINT_TYPE IN ('P', 'R', 'U', 'C')
ORDER BY TABLE_NAME, CONSTRAINT_TYPE;

PROMPT
PROMPT Indexes:

SELECT 
    INDEX_NAME,
    TABLE_NAME,
    UNIQUENESS,
    STATUS
FROM DBA_INDEXES
WHERE OWNER = 'RETAIL'
ORDER BY TABLE_NAME, INDEX_NAME;

PROMPT
PROMPT Sequences:

SELECT 
    SEQUENCE_NAME,
    LAST_NUMBER,
    INCREMENT_BY
FROM DBA_SEQUENCES
WHERE SEQUENCE_OWNER = 'RETAIL';

-- ===================================================================
-- COMPLETION SUMMARY
-- ===================================================================
PROMPT
PROMPT ===================================================================
PROMPT RETAIL SCHEMA CREATION COMPLETE
PROMPT ===================================================================
PROMPT
PROMPT Tables Created:
PROMPT   ✓ CUSTOMERS (10 columns, Primary Key, indexes)
PROMPT   ✓ ORDERS (9 columns, Primary Key, Foreign Key to CUSTOMERS, indexes)
PROMPT   ✓ PRODUCTS (10 columns, Primary Key, indexes)
PROMPT
PROMPT Constraints:
PROMPT   ✓ Primary keys on all tables
PROMPT   ✓ Foreign key: ORDERS.CUSTOMER_ID → CUSTOMERS.CUSTOMER_ID
PROMPT   ✓ Unique constraint on CUSTOMERS.EMAIL
PROMPT   ✓ Check constraints for data validation
PROMPT
PROMPT Indexes:
PROMPT   ✓ Primary key indexes (automatic)
PROMPT   ✓ Foreign key indexes
PROMPT   ✓ Performance indexes on frequently queried columns
PROMPT
PROMPT CDC Configuration:
PROMPT   ✓ Supplemental logging enabled on all tables
PROMPT
PROMPT Sequences:
PROMPT   ✓ SEQ_CUSTOMER_ID (starts at 1000)
PROMPT   ✓ SEQ_ORDER_ID (starts at 10000)
PROMPT   ✓ SEQ_PRODUCT_ID (starts at 100)
PROMPT
PROMPT Next Steps:
PROMPT   1. Review table structure (DESC RETAIL.CUSTOMERS)
PROMPT   2. Load sample data (run 06_insert_sample_data.sql)
PROMPT   3. Test CDC operations (run 07_cdc_test_operations.sql)
PROMPT   4. Proceed to Databricks Lakeflow Connect setup
PROMPT ===================================================================

EXIT;
