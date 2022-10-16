-- CREATE TABLE IF NOT EXISTS transactions (
--     transaction_id CHARACTER VARYING (25) PRIMARY KEY,
--     date CHARACTER VARYING (25),
--     card CHARACTER VARYING (25),
--     account CHARACTER VARYING (25),
--     account_valid_to CHARACTER VARYING (25),
--     client CHARACTER VARYING (10),
--     last_name CHARACTER VARYING (25),
--     first_name CHARACTER VARYING (25),
--     patronymic CHARACTER VARYING (25),
--     date_of_birth CHARACTER VARYING (25),
--     passport BIGINT,
--     passport_valid_to CHARACTER VARYING (25),
--     phone CHARACTER VARYING (15),
--     oper_type CHARACTER VARYING (15),
--     amount REAL,
--     oper_result CHARACTER VARYING (15),
--     terminal CHARACTER VARYING (15),
--     terminal_type CHARACTER VARYING (10),
--     city CHARACTER VARYING (25),
--     address CHARACTER VARYING (100)
-- );
CREATE USER docker;
CREATE DATABASE docker;
GRANT ALL PRIVILEGES ON DATABASE docker TO docker;
CREATE TABLE monke(monke INT);