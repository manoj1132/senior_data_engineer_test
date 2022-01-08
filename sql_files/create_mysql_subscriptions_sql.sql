CREATE TABLE IF NOT EXISTS subscriptions
( id INT,
  end_date TIMESTAMP,
  amount FLOAT,
  status VARCHAR(10),
  created_at TIMESTAMP,
  start_date TIMESTAMP
  );
  
  GRANT SELECT on subscriptions TO <user_name>;
