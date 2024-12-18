
ref: https://registry.opendata.aws/copernicus-dem/
---

### Create Items

```bash
# Install dependencies
python -m pip install pip -U
python -m pip install rio-stac

# Create STAC collections and items (~30 min)
cd Copernicus-Dem
aws s3 ls copernicus-dem-30m/ --recursive | awk '{print "s3://copernicus-dem-30m/"$NF}'  | grep "_DEM.tif" |
    parallel -j 20 rio stac {} -c "copernicus-dem"  -d "2022-05-09" --without-raster --without-eo --asset-mediatype COG -n dem | jq -c 'del(.properties."proj:geometry") | del(.properties."proj:bbox")' >  items.json

# manually edit
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
