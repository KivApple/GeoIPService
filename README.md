# GeoIP Service

Service that serves MaxMind GeoIP database

## Launch

    java -Dconfig.file=/path/to/config -jar /path/to/jar 

## Configuration file example

    host = "127.0.0.1"
    port = 8080
    temp-dir = "./tmp"
    
    database {
        url = "jdbc:postgresql://localhost:5432/DATABASE?user=USER&password=PASSWORD"
    }
    
    max-mind {
        license-key = "YOUR_LICENSE_KEY"
        download-url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key=@&suffix=zip"
        update-interval-days = 3 # Optional
    }

**Note:** Only PostgreSQL database URL is supported at the moment.

## Endpoints

| Endpoint | Description |
|----------|-------------|
|`GET /ip/{localeCode}/{address}`|Search location by IPv4 or IPv6 address|
|`GET /location/{localeCode}/{name}`|Search 10 locations with names are starting with given string|
|`GET /locales`|Returns the list of all supported locales|
|`GET /status`|Returns current status of the service (last update date, running update status)|
|`POST /update`|Perform database update (async, use `/status` to monitor progress)|

There are also `/docs/docs.yaml` with OpenAPI description of the service and Swagger UI on `/docs`.
