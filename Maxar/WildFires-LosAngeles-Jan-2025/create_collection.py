"""Create STAC Items file."""

import json
from copy import deepcopy

import click
import pystac

template = {
    "type": "Collection",
    "id": "",
    "stac_version": "1.0.0",
    "description": "Maxar OpenData",
    "links": [],
    "title": "",
    "extent": {
        "spatial": {
            "bbox": [],
        },
        "temporal": {
            "interval": [
                ["2017-10-20T16:35:02Z", "2023-08-30T19:19:59Z"]
            ]
        }
    },
    "license": "CC-BY-NC-4.0",
    "stac_extensions": [
        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
        "https://stac-extensions.github.io/render/v1.0.0/schema.json"
    ],
    "item_assets": {},
    "renders": {
        "visual": {
            "title": "Visual Image",
            "assets": [ "visual" ],
            "asset_bidx": "visual|1,2,3",
            "minmax_zoom": [8, 22]
        }
    }
}


@click.command()
@click.option('--items', "items_path", type=str)
@click.option('--collection', "collection_id", type=str)
@click.option('--output', "output_path", type=str, default="collections.json")
def main(items_path, collection_id, output_path):
    with open(items_path, "r") as fin:
        items = [json.loads(f.strip("\n")) for f in fin.readlines()]

    collection_assets = {}
    for item in items:
        collection_assets.update(
            **{
                name: {
                    "type": values["type"],
                    "title": values["title"],
                    "roles": values.get("roles", ["data"]),
                }
                for name, values in item["assets"].items()
            }
        )

    minx, miny, maxx, maxy = zip(*[x["bbox"] for x in items])
    datetimes = [x["properties"]["datetime"] for x in items]

    collection = deepcopy(template)
    collection["id"] = collection_id
    collection["title"] = collection_id
    collection["item_assets"] = collection_assets

    collection["extent"]["spatial"]["bbox"] = [
        [min(minx), min(miny), max(maxx), max(maxy)],
        [min(minx), min(miny), max(maxx), max(maxy)],
    ]

    collection["extent"]["temporal"]["interval"] = [
        [min(datetimes), max(datetimes)],
    ]

    with open(output_path, "w") as f:
        f.write(json.dumps(collection))

if __name__ == '__main__':
    main()

