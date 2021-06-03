DROP TABLE STG_ORDERS

CREATE TABLE STG_ORDERS(
ORDER_ID LONG
, USER_ID LONG
, ORDER_NUMBER BIGINT
, ORDER_DOW INT
, ORDER_HOUR_OF_DAY INT
, PRODUCT VARCHAR(252)
, AISLE VARCHAR(252)
, DEPARTMENT VARCHAR(252)
, ADD_TO_CART_ORDER INT
, DAYS_SINCE_PRIOR_ORDER INT
);

SELECT * FROM STG_ORDERS

SELECT COUNT(*) FROM STG_ORDERS

SELECT DISTINCT DEPARTMENT FROM STG_ORDERS

SELECT * FROM STG_ORDERS WHERE ISNULL(DEPARTMENT) = 1

#TRUNCATE TABLE STG_ORDERS