CREATE TABLE IF NOT EXISTS messages
( created_at TIMESTAMP,
  receiver_id INT,
  id INT,
  sender_id INT
  );
  
  GRANT SELECT on messages TO <user_name>;
