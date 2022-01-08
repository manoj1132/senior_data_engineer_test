BEGIN TRANSACTION;

CREATE TEMP TABLE messages_temp LIKE messages;

LOAD (LOCAL) DATA INFILE '/absolute_path>/messages.csv' INTO TABLE messages_temp FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROW;

DELETE FROM messages_temp
USING messages
WHERE messages.id = messages_temp.id
AND messages.created_at = messages_temp.created_at
AND messages.receiver_id = messages_temp.receiver_id
AND messages.id = messages_temp.id
AND messages.sender_id = messages_temp.sender_id;

DELETE FROM messages
USING messages_temp
WHERE messages_temp.id = messages.id;

INSERT INTO messages
SELECT * from messages_temp;

END TRANSACTION;


