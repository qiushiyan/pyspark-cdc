UPDATE cdc.users SET last_login = '2021-10-15' WHERE user_id = 6;
INSERT INTO cdc.users VALUES (133,'Bob Ding', 'New York', "2021-10-17");
INSERT INTO cdc.users VALUES (134,'Ross Green', 'Seattle', '2021-10-17');
INSERT INTO cdc.users VALUES (135,'Monica Bing', 'Portland', '2021-10-17');
UPDATE cdc.users SET name = 'Kevin Durant', last_login = '2021-10-14' WHERE user_id = 8;
UPDATE cdc.users SET city = 'New York', last_login = '2021-10-17' WHERE user_id = 9;
DELETE FROM cdc.users WHERE user_id = 10;
