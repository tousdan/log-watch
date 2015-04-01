CREATE TABLE transaction_details (
    transaction_id  varchar(36) REFERENCES transactions (transaction_id),
    detail text non null,
    time timestamp without timezone non null
);