import json
import click
import pystac
import httpx

COLLECTION_ID_UPSTREAM = "sentinel-2-l2a"
COLLECTION_ID_DOWNSTREAM = "sentinel-2-iceland"
COLLECTION_BBOX = [-24.95, 63.38, -13.99, 66.56]


@click.command()
@click.option("--collections", "collections_path", type=str, default="collections.json")
@click.option("--items", "items_path", type=str, default="items.json")
@click.option('--with-s3-urls/--without-s3-url', type=bool, default=False)
def main(collections_path, items_path, with_s3_urls):
    click.echo("Connecting to static catalog...")
    try:
        catalog = pystac.Catalog.from_file("https://earth-search.aws.element84.com/v1/")
    except Exception as e:
        print(f"Error loading catalog: {e}")
        return

    with open(collections_path, "a") as f_col, open(items_path, "a") as f_itm:
        for collection in catalog.get_collections():
            # Ignore all but one collection
            if collection.id != COLLECTION_ID_UPSTREAM:
                continue

            print(f"Looking for Items in {collection.id} Collection")

            try:
                # Load the collection from URL
                collection = pystac.Collection.from_file(
                    f"https://earth-search.aws.element84.com/v1/collections/{collection.id}"
                )
                print(f"Collection loaded successfully: {collection.id}")
            except Exception as e:
                print(f"Error loading collection data for {collection.id}: {e}")
                continue

            # Obtain items from Earth Search API
            url = "https://earth-search.aws.element84.com/v1/search"
            payload = {
                "collections": [COLLECTION_ID_UPSTREAM],
                "bbox": COLLECTION_BBOX,
                "datetime": "2023-01-01T00:00:00Z/2023-12-31T23:59:59Z",
                "limit": 500,
                "query": {"eo:cloud_cover": {"lt": 5}},
            }

            try:
                with httpx.Client() as client:
                    response = client.post(url, json=payload)
                    response.raise_for_status()
            except httpx.RequestError as e:
                print(f"Error during request: {e}")
                continue
            except httpx.HTTPStatusError as e:
                print(f"HTTP error occurred: {e}")
                continue

            data = response.json()

             # Loop through each items
             # edit items and save into a top level collection JSON file
            for item_dict in data.get("features", []):

                print(f"Fetched Item ID: {item_dict['id']}")

                item_dict["collection"] = COLLECTION_ID_DOWNSTREAM
                item_dict["links"] = []

                # Filter out assets with 'jp2' in their href
                if "assets" in item_dict:
                    filtered_assets = {k: v for k, v in item_dict["assets"].items() if 'jp2' not in v["href"]}
                    item_dict["assets"] = filtered_assets

                # Replace HTTP URLs with S3 URLs
                if with_s3_urls:
                    for asset in item_dict["assets"].values():
                        asset["href"] = asset["href"].replace("https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs", "s3://sentinel-cogs/sentinel-s2-l2a-cogs")

                 # Remove additional metadata
                item_dict["properties"] = {k: v for k, v in item_dict["properties"].items() if not k.startswith("s2:")}
                item_dict["properties"] = {k: v for k, v in item_dict["properties"].items() if not k.startswith("earthsearch:")}

                f_itm.write(json.dumps(item_dict) + "\n")

            c = collection.to_dict()
            c["links"] = []
            c["description"] = "Sentinel-2 L2A images over Iceland"
            c["id"] = COLLECTION_ID_DOWNSTREAM

            # set collection extent to the bounding box of Iceland
            c["extent"]["spatial"]["bbox"] = [COLLECTION_BBOX]

            # Filter out 'jp2' assets
            if "item_assets" in c:
                filtered_assets = {k: v for k, v in c["item_assets"].items() if 'jp2' not in v["type"]}
                c["item_assets"] = filtered_assets

            f_col.write(json.dumps(c) + "\n")


if __name__ == "__main__":
    main()
