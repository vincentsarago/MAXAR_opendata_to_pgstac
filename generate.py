"""Create STAC Collections and Items files."""

import json
import pystac


print("Connecting to static catalog...")
catalog = pystac.Catalog.from_file("https://maxar-opendata.s3.amazonaws.com/events/catalog.json")

collections = list(catalog.get_collections())
print(f"Found {len(collections)} collections")
print(collections)

print("Creating collections.json file...")
with open("collections.json", "w") as f:
    for collection in collections:
        c = collection.to_dict()
        c["links"] = []
        c["id"] = "MAXAR_" + c["id"].replace("-","_")
        c["description"] = "Maxar OpenData | " + c["description"]
        f.write(json.dumps(c) + "\n")


print("Creating items .json files...")
for collection in collections:
    collection_id = "MAXAR_" + collection.id.replace("-","_")
    print(collection_id)
    with open(f"{collection_id}_items.json", "w") as f:

        # Each Collection has collections
        for c in collection.get_collections():

            # Loop through each items
            # edit items and save into a top level collection JSON file
            for item in c.get_all_items():
                item_dict = item.make_asset_hrefs_absolute().to_dict()
                item_dict["links"] = []
                item_dict["collection"] = collection_id
                item_dict["id"] = item.id.replace("/", "_")
                item_str = json.dumps(item_dict)
                f.write(item_str + "\n")
