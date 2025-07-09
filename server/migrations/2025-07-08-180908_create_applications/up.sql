-- Your SQL goes here
CREATE TABLE `applications`(
	`id` INTEGER NOT NULL PRIMARY KEY,
	`user` TEXT NOT NULL,
	`token` TEXT NOT NULL,
	`addr` TEXT
);

