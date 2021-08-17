CREATE DATABASE proddatabase;
\c proddatabase
CREATE SCHEMA images;
CREATE TABLE IF NOT EXISTS images.avatars (
  id SERIAL PRIMARY KEY,
  path VARCHAR
);
