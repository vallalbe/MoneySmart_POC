select distinct * from orders limit 5;

    
/* -- Top 10 Selling products or best seller with total count -- */
SELECT ProductName, SUM(Quantity) AS TotalQuantity
FROM orders
GROUP BY ProductName
ORDER BY TotalQuantity DESC
LIMIT 10;

/* -- Top 10 Selling products or best seller -- */
select ProductName from (
SELECT ProductName, SUM(Quantity) AS TotalQuantity
FROM orders
GROUP BY ProductName
ORDER BY TotalQuantity DESC
LIMIT 10) x;


/* --  products bought together with occurance-- */
SELECT c.original_SKU, c.bought_with, count(*) as times_bought_together
FROM (
  SELECT a.ProductName as original_SKU, b.ProductName as bought_with
  FROM orders a
  INNER join orders b
  ON a.OrderID = b.OrderID AND a.ProductName != b.ProductName) c
GROUP BY c.original_SKU, c.bought_with;


/* --  products(ProductB) frequently purchased with top 10 bestsellers(ProductA) -- */
SELECT a.ProductName AS ProductA, b.ProductName AS ProductB, count(*) as Occurrences
FROM orders AS a
INNER JOIN orders AS b ON a.OrderID = b.OrderID
AND a.ProductName != b.ProductName 
where a.ProductName IN (select ProductName from (
SELECT ProductName, SUM(Quantity) AS TotalQuantity
FROM orders
GROUP BY ProductName
ORDER BY TotalQuantity DESC
LIMIT 10) as b) 
GROUP BY ProductA,ProductB order by Occurrences desc;

--  output exported into Q2_itempair_tmp.csv 





