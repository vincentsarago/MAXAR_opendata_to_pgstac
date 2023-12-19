"""Create STAC Collections and Items files."""

import json
import time
from concurrent import futures
from typing import Any, Dict, Sequence, Type, Union, Tuple
from pathlib import Path

import httpx
import click
import pystac


def retry(
    tries: int,
    exceptions: Union[Type[Exception], Sequence[Type[Exception]]] = Exception,
    delay: float = 0.0,
):
    """Retry Decorator"""

    def _decorator(func: Any):
        def _newfn(*args: Any, **kwargs: Any):

            attempt = 0
            while attempt < tries:
                try:
                    return func(*args, **kwargs)
                except exceptions:  # type: ignore
                    attempt += 1
                    time.sleep(delay)

            return func(*args, **kwargs)

        return _newfn

    return _decorator


@retry(tries=3, delay=1)
def get_collection(url) -> pystac.Collection:
    return pystac.Collection.from_file(url)


@retry(tries=3, delay=1)
def get_item(url: str) -> Tuple[str, Dict]:
    resp = httpx.get(url)
    resp.raise_for_status()
    return url, resp.json()


@click.command()
@click.option('--collections', "collections_path", type=str, default="collections.json")
@click.option('--items', "items_path", type=str, default="items.json")
@click.option('--with-s3-urls/--without-s3-url', type=bool, default=False)
@click.option('--with-assets-extension/--without-assets-extension', type=bool, default=False)
def main(collections_path, items_path, with_s3_urls, with_assets_extension):
    click.echo("Connecting to static catalog...")

    catalogs = pystac.Catalog.from_file(
        "https://nz-imagery.s3-ap-southeast-2.amazonaws.com/catalog.json"
    )
    collection_links = [
        link.absolute_href
        for link in catalogs.get_child_links()
    ]

    with futures.ThreadPoolExecutor(max_workers=50) as executor:
        collections = list(executor.map(get_collection, collection_links))

    previous_col = []
    if Path(collections_path).exists():
        with open(collections_path, "r") as f:
            previous_col = [json.loads(l).get("id") for l in f.readlines()]

        print(f"{len(previous_col)} collections already found in {collections_path}")

    with open(collections_path, "a") as f_col, open(items_path, "a") as f_itm:
        for collection in collections:
            collection_id = "LINZ_" + collection.id.replace("-","_")
            if collection_id in previous_col:
                continue

            collection_assets = {}

            print(f"Looking for Items in {collection_id} Collection")

            items_links = [
                link.absolute_href
                for link in collection.get_item_links()
            ]

            with futures.ThreadPoolExecutor(max_workers=50) as executor:
                future_to_item = [
                    executor.submit(get_item, link) for link in items_links
                ]
                with click.progressbar(
                    futures.as_completed(future_to_item),
                    length=len(items_links),
                    show_percent=True,
                    show_pos=True,
                ) as future:
                    for res in future:
                        url, item_dict = res.result()
                        item_id = item_dict['id']
                        root_path = url.replace(f"{item_id}.json", "")

                        item_dict["links"] = []
                        item_dict["collection"] = collection_id
                        item_dict["id"] = item_id.replace("/", "_")
                        if with_s3_urls:
                            if "stac_extensions" not in item_dict:
                                item_dict["stac_extensions"] = []

                            item_dict["stac_extensions"].append("https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json")
                            for asset in item_dict["assets"]:
                                ori = item_dict["assets"][asset]["href"].replace("./", root_path)
                                item_dict["assets"][asset]["href"] = ori.replace("https://nz-imagery.s3-ap-southeast-2.amazonaws.com", "s3://nz-imagery")
                                item_dict["assets"][asset]["alternate"] = {
                                    "public": {
                                        "title": "Public Access",
                                        "href": ori,
                                    }
                                }

                        collection_assets.update(
                            **{
                                name: {
                                    "type": values.get("type", "unknown"),
                                    "title": values.get("title", name),
                                    "roles": values.get("roles", ["data"]),
                                }
                                for name, values in item_dict["assets"].items()
                            }
                        )

                        f_itm.write(json.dumps(item_dict) + "\n")

            c = collection.to_dict()
            c["links"] = []
            c["id"] = collection_id
            c["description"] = "LINZ OpenData | " + c["description"]

            if with_assets_extension:
                if not c.get("stac_extensions"):
                    c["stac_extensions"] = []
                c["stac_extensions"].append("https://stac-extensions.github.io/item-assets/v1.0.0/schema.json")
                c["item_assets"] = collection_assets

            f_col.write(json.dumps(c) + "\n")




if __name__ == '__main__':
    main()
