CREATE TABLE geoip_locations (
    id BIGINT NOT NULL,
    locale_code VARCHAR(5) NOT NULL,
	parent_id BIGINT,
    level VARCHAR(16) NOT NULL,
	name VARCHAR(256),
	code VARCHAR(3),
	is_in_european_union BOOLEAN,

	PRIMARY KEY (id, locale_code),
	FOREIGN KEY (parent_id, locale_code) REFERENCES geoip_locations(id, locale_code)
);
