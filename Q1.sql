/* Create table */
CREATE TABLE pageviews (  
ID int(11) NOT NULL,
User_ID int(11) NOT NULL,
Page_ID int(11) DEFAULT NULL,
Visit_Date date DEFAULT NULL,
Visit_Time time DEFAULT NULL,
PRIMARY KEY (ID),
UNIQUE KEY ID_UNIQUE (ID)
) ;

/* load data into table from file */
LOAD DATA INFILE '/tmp/pageviews.csv' 
INTO TABLE pageviews 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

/* Verify data on table */
select * from pageviews order by USer_ID,Page_ID,Visit_Time;

/* add few more data mannual insert option */
-- insert into pageviews values(19,3,54,'2018-01-01','13:13:34'); # used to test different test cases


/*  No matters like  session continuous or not */
SELECT 
    Page_ID,
    DATE(CONCAT(CONCAT(Visit_Date, ' '), Visit_Time)) Visited_Date,
    COUNT(1) Total_User_Sessions
FROM
    pageviews
GROUP BY Page_ID , Visited_Date
ORDER BY Page_ID , Visited_Date , Total_User_Sessions;



--  (A user session is defined as continuous activity on a site where each activity is within 10 mins of each other.)

/* Page_ID, s_start, s_next  */
SELECT Page_ID,dt as s_start,LEAD(dt,1) OVER (
        PARTITION BY Page_ID
        ORDER BY dt ) s_next
FROM (SELECT 
    Page_ID,
    CONCAT(CONCAT(Visit_Date, ' '), Visit_Time) dt
FROM
    pageviews
GROUP BY Page_ID , dt
ORDER BY Page_ID , dt)as a;



/* Page_ID, s_start, n_next, duration, isGreaterThanTenMins */

 select  Page_ID,  s_start,n_next,TIMESTAMPDIFF(MINUTE,   s_start,n_next)  as duration,
 CASE 
 WHEN TIMESTAMPDIFF(MINUTE,   s_start,n_next) IS NULL THEN  1
 WHEN TIMESTAMPDIFF(MINUTE,   s_start,n_next) >= 10 THEN  1
 ELSE 0
 END as isGreaterThanTenMins
 FROM
(select Page_ID,dt as s_start,LEAD(dt,1) OVER (
        PARTITION BY Page_ID
        ORDER BY dt ) n_next
FROM (SELECT 
    Page_ID,
    CONCAT(CONCAT(Visit_Date, ' '), Visit_Time) dt
FROM
    pageviews p
GROUP BY Page_ID , dt
ORDER BY Page_ID , dt)as a)as b;


/* Page_ID, Visit_Date, Total_User_Sessions */
 select Page_ID, date(s_start) as Visit_Date,sum(diff) Total_User_Sessions
 from (select  Page_ID,  s_start,n_next,TIMESTAMPDIFF(MINUTE,   s_start,n_next) ,
 CASE 
 WHEN TIMESTAMPDIFF(MINUTE,   s_start,n_next) IS NULL THEN  1
 WHEN TIMESTAMPDIFF(MINUTE,   s_start,n_next) >=10 THEN  1
 ELSE 0
 END as diff
 FROM
(select Page_ID,dt as s_start,LEAD(dt,1) OVER (
        PARTITION BY Page_ID
        ORDER BY dt ) n_next
FROM (SELECT 
    Page_ID,
    CONCAT(CONCAT(Visit_Date, ' '), Visit_Time) dt
FROM
    pageviews p
GROUP BY Page_ID , dt
ORDER BY Page_ID , dt)as a)as b)as c group by Page_ID,Visit_Date  order by Page_ID,Visit_Date;



