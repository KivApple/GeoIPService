CREATE TABLE geoip_timezones (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE
);

ALTER TABLE geoip_locations ADD COLUMN timezone_id BIGINT REFERENCES geoip_timezones(id);
