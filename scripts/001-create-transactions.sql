CREATE TABLE transactions (
  transaction_id  varchar(36) PRIMARY KEY,
  environment     varchar(4),
  username        varchar(50),
  date            date
);