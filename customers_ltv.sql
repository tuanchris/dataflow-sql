SELECT
  o.customer_id,
  SUM(o.amount) ltv
FROM
  `demobox-313313.ecommerce.orders` o
LEFT JOIN
  `demobox-313313.ecommerce.customers` c
ON
  o.customer_id = c.customer_id
WHERE o.order_status = 'Closed'
GROUP BY
  1
ORDER BY
  2 DESC