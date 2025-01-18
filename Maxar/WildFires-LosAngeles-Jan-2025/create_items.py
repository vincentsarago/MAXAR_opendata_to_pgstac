"""Create STAC Items file."""

import json

import click
import pystac

collection_id = "WildFires-LosAngeles-Jan-2025"

@click.command()
@click.option('--list', "stac_path", type=str, default="list_items.txt")
@click.option('--output', "output_path", type=str, default="items.json")
@click.option('--with-s3-urls/--without-s3-url', type=bool, default=False)
def main(stac_path, output_path, with_s3_urls):
    with open(stac_path, "r") as fin:
        item_paths = [f for f in fin.readlines()]

    with open(output_path, "a") as f_itm:
        # Loop through each items
        # edit items and save into a top level collection JSON file
        for p in item_paths:
            item = pystac.Item.from_file(p)
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

            f_itm.write(json.dumps(item_dict) + "\n")

if __name__ == '__main__':
    main()

