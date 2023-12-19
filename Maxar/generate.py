"""Create STAC Collections and Items files."""

import json
from pathlib import Path

import click
import pystac


@click.command()
@click.option('--collections', "collections_path", type=str, default="collections.json")
@click.option('--items', "items_path", type=str, default="items.json")
@click.option('--with-s3-urls/--without-s3-url', type=bool, default=False)
@click.option('--with-assets-extension/--without-assets-extension', type=bool, default=False)
def main(collections_path, items_path, with_s3_urls, with_assets_extension):
    click.echo("Connecting to static catalog...")
    catalog = pystac.Catalog.from_file("https://maxar-opendata.s3.amazonaws.com/events/catalog.json")

    previous_col = []
    if Path(collections_path).exists():
        with open(collections_path, "r") as f:
            previous_col = [json.loads(l).get("id") for l in f.readlines()]

        print(f"{len(previous_col)} collections already found in {collections_path}")

    with open(collections_path, "a") as f_col, open(items_path, "a") as f_itm:
        for collection in catalog.get_collections():
            collection_id = "MAXAR_" + collection.id.replace("-","_")
            if collection_id in previous_col:
                continue

            collection_assets = {}

            print(f"Looking for Items in {collection_id} Collection")

            # Loop through each items
            # edit items and save into a top level collection JSON file
            for item in collection.get_items():
                item_dict = item.make_asset_hrefs_absolute().to_dict()
                item_dict["links"] = []
                item_dict["collection"] = collection_id
                item_dict["id"] = item.id.replace("/", "_")
                if with_s3_urls:
                    item_dict["stac_extensions"].append("https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json")
                    for asset in item_dict["assets"]:
                        ori = item_dict["assets"][asset]["href"]
                        item_dict["assets"][asset]["href"] = ori.replace("https://maxar-opendata.s3.amazonaws.com", "s3://maxar-opendata")
                        item_dict["assets"][asset]["alternate"] = {
                            "public": {
                                "title": "Public Access",
                                "href": ori,
                            }
                        }

                    collection_assets.update(
                        **{
                            name: {
                                "type": values["type"],
                                "title": values["title"],
                                "roles": values.get("roles", ["data"]),
                            }
                            for name, values in item_dict["assets"].items()
                        }
                    )

                    f_itm.write(json.dumps(item_dict) + "\n")

            c = collection.to_dict()
            c["links"] = []
            c["id"] = collection_id
            c["description"] = "Maxar OpenData | " + c["description"]

            if with_assets_extension:
                if not c.get("stac_extensions"):
                    c["stac_extensions"] = []
                c["stac_extensions"].append("https://stac-extensions.github.io/item-assets/v1.0.0/schema.json")
                c["item_assets"] = collection_assets

            f_col.write(json.dumps(c) + "\n")




if __name__ == '__main__':
    main()
