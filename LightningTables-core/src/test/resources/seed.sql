-- Seed users table
INSERT INTO users (first_name, last_name) VALUES ('John', 'Doe');
INSERT INTO users (first_name, last_name) VALUES ('Jane', 'Smith');
INSERT INTO users (first_name, last_name) VALUES ('Bob', 'Johnson');

-- Seed transactions table
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 1);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 0.05);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 2);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'CREDIT', 'CALL', 3);
INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 4);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'CALL', 5);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'SMS', 6);
INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'DATA', 7);
INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'DEBIT', 'CALL', 8);
INSERT INTO transactions (user_id, type, service, cost) VALUES (3, 'CREDIT', 'SMS', 9.3);

