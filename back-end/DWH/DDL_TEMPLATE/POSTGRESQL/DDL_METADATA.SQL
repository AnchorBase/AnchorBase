
CREATE ROLE anchorbase WITH SUPERUSER LOGIN PASSWORD '1234' ENCODING 'UTF8';

CREATE DATABASE "anchorbase_metadata";

CREATE EXTENSION "uuid-ossp";

CREATE TABLE "source" (
    id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "idmap_table" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "idmap_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue_table" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "anchor_table" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "anchor_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "attribute_table" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "attribute_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "tie_table" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "tie_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);