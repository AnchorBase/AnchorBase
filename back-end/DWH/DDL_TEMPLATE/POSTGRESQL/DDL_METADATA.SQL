
CREATE ROLE anchorbase WITH SUPERUSER LOGIN PASSWORD '1234' ENCODING 'UTF8';

CREATE DATABASE "anchorbase_metadata";

CREATE EXTENSION "uuid-ossp";

CREATE TABLE "source" (
    id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "entity" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "entity_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "idmap" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "idmap_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "anchor" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "anchor_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "attribute" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "attribute_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "tie" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "tie_column" (
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue_increment"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "queue_etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "idmap_etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "anchor_etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "attribute_etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);
CREATE TABLE "tie_etl"
(
	id uuid PRIMARY KEY,
	value json NOT NULL
);


