CREATE TABLE IF NOT EXISTS users
( created_at TIMESTAMP,
  updated_at TIMESTAMP,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  address VARCHAR(50),
  city VARCHAR(50),
  zipcode VARCHAR(50),
  email VARCHAR(50),
  birth_date TIMESTAMP,
  id INT,
  profile_gender VARCHAR(10),
  profile_issmoking BOOLEAN,
  profile_profession VARCHAR(100),
  profile_income DOUBLE
  );
  
  GRANT SELECT on users TO <user_name>;
