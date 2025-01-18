
1. List all Items

```
aws s3 ls maxar-opendata/events/WildFires-LosAngeles-Jan-2025/ard/11/ --recursive | grep ".json" | awk '{print "https://maxar-opendata.s3.amazonaws.com/"$NF}' > list_items.txt
```

2. Create STAC Items

```
python create_items.py --list list_items.txt --output items.json --with-s3-urls
```

3. Create Collection

```
python create_collection.py --items items.json --name WildFires-LosAngeles-Jan-2025 --output collections.json
```

4. Ingest collection

# Ingest Collections and Items
pypgstac load collections collections.json --dsn postgresql://username:password@0.0.0.0:5439/postgis --method insert_ignore
pypgstac load items items.json --dsn postgresql://username:password@0.0.0.0:5439/postgis --method insert_ignore
```
