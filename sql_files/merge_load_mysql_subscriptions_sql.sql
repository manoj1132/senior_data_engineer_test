BEGIN TRANSACTION;

CREATE TEMP TABLE subscriptions_temp LIKE subscriptions;

LOAD (LOCAL) DATA INFILE '/absolute_path>/subscriptions.csv' INTO TABLE subscriptions_temp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROW;

DELETE FROM subscriptions_temp
USING subscriptions
WHERE subscriptions.id = subscriptions_temp.id
AND subscriptions.end_date = subscriptions_temp.end_date
AND subscriptions.amount = subscriptions_temp.amount
AND subscriptions.status = subscriptions_temp.status
AND subscriptions.created_at = subscriptions_temp.created_at
AND subscriptions.start_date = subscriptions_temp.start_date;

DELETE FROM subscriptions
USING subscriptions_temp
WHERE subscriptions_temp.id = subscriptions.id;

INSERT INTO subscriptions
SELECT * from subscriptions_temp;

END TRANSACTION;


