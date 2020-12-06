CREATE TABLE geoip_locales(
    id BIGSERIAL,
    code VARCHAR(5) NOT NULL UNIQUE
);
