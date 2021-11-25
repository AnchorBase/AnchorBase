
CREATE ROLE anchorbase with SUPERUSER LOGIN PASSWORD '1234';

CREATE DATABASE "anchorbase_metadata";

CREATE EXTENSION "uuid-ossp";

CREATE TABLE "source" (
    id uuid PRIMARY KEY,
	value json NULL
);