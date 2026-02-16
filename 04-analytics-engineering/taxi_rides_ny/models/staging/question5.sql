SELECT
    SUM(total_monthly_trips) AS total_trips
FROM
    taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE
    service_type = 'Green'
    AND
    year(revenue_month) = 2019
    AND
    month(revenue_month) = 10
