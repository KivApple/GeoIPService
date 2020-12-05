CREATE TABLE geoip_blocks (
    id BIGSERIAL,
    start CHAR(32) NOT NULL,
    stop CHAR(32) NOT NULL,
    location_id BIGINT,
    postal_code VARCHAR(10),
    latitude REAL,
    longitude REAL,
    accuracy_radius FLOAT,
	is_anonymous_proxy BOOLEAN NOT NULL,
	is_satellite_provider BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS stop_start ON geoip_blocks(stop, start);
