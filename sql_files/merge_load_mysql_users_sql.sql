BEGIN TRANSACTION;

CREATE TEMP TABLE users_temp LIKE users;

LOAD DATA INFILE '/absolute_path>/users.csv' INTO TABLE users_temp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROW;

DELETE FROM users_temp
USING users
WHERE users.id = users_temp.id
AND users.updated_at >= users_temp.updated_at;

DELETE FROM users
USING users_temp
WHERE users.id = users_temp.id;

INSERT INTO users
SELECT * from users_temp;

END TRANSACTION;


