CREATE USER sre_user WITH PASSWORD '<change_password>';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA images TO sre_user;
GRANT CONNECT ON DATABASE "proddatabase" to sre_user;
