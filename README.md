# Backup_bot

Для создания пустой базы под дамп используйте:

 MySQL:
 
CREATE DATABASE databasename;
GRANT ALL PRIVILEGES ON databasename.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;

Postgre:

CREATE DATABASE databasename WITH OWNER your_user ENCODING 'UTF8';
\c databasename
CREATE SCHEMA public;
ALTER SCHEMA public OWNER TO your_user;
GRANT ALL ON SCHEMA public TO your_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO your_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO your_user;
\q
