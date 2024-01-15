
<p align="center">
  <img width="700" src="https://github.com/stac-utils/titiler-pgstac/assets/10407788/7623471f-9391-4822-8799-b552775444fa"/>
  <p align="center">STAC Collections/Items for MAXAR / Umbra / Linz OpenData.</p>
</p>

---

### Fetch STAC Collection/Items

The goal is to crawl the static collections/items and fetch save in order to ingest them in pgSTAC

```bash
# Install dependencies
python -m pip install pip -U
python -m pip install pystac httpx click

# Create STAC collections and items (~30 min)
cd Maxar
python -m generate --collections collections.json --items items.json --with-assets-extension --with-s3-urls
```

### Ingest in pgSTAC

```bash
# Install requirement
python -m pip install pypgstac psycopg[pool]

# Launch pgstac database
docker-compose up -d database

# Check the database connection
pypgstac pgready --dsn postgresql://username:password@0.0.0.0:5439/postgis

# Ingest Collections and Items
pypgstac load collections collections.json --dsn postgresql://username:password@0.0.0.0:5439/postgis --method insert_ignore
pypgstac load items items.json --dsn postgresql://username:password@0.0.0.0:5439/postgis --method insert_ignore
```
