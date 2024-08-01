USE flights_data;

DROP TABLE IF EXISTS flights_subset;

CREATE TABLE flights_subset (
    flight VARCHAR(10),
    planned_time DATETIME,
    destination VARCHAR(100),
    gate VARCHAR(10),
    company VARCHAR(100),
    revised_time DATETIME
);

INSERT INTO flights_subset (flight, planned_time, destination, gate, company, revised_time)
SELECT flight,
DATE_FORMAT(FROM_UNIXTIME(planned), '%Y-%m-%d %H:%i'),
destination,
gate,
company,
DATE_FORMAT(FROM_UNIXTIME(revised), '%Y-%m-%d %H:%i')
FROM flights
WHERE gate BETWEEN 62 AND 68
AND DATE(FROM_UNIXTIME(planned)) = '2024-07-30'
order by planned;

SELECT * FROM flights_subset;