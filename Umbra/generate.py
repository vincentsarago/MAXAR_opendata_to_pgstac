"""Create STAC Collections and Items files."""

import json
import time
from concurrent import futures
from typing import Any, Dict, Sequence, Type, Union

import click
import httpx
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
def _get_item(url: str) -> Dict:
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.json()


@click.command()
@click.option("--collections", "collections_path", type=str, default="collections.json")
@click.option("--items", "items_path", type=str, default="items.json")
@click.option("--with-s3-urls/--without-s3-url", type=bool, default=False)
@click.option(
    "--with-assets-extension/--without-assets-extension", type=bool, default=False
)
def main(collections_path, items_path, with_s3_urls, with_assets_extension):
    click.echo("Connecting to static catalog...")

    top_catalogs = pystac.Catalog.from_file(
        "https://s3.us-west-2.amazonaws.com/umbra-open-data-catalog/stac/catalog.json"
    )
    sub_catalogs = [
        pystac.Catalog.from_file(link.absolute_href)
        for link in top_catalogs.get_child_links()
    ]

    with open(collections_path, "a") as f_col, open(items_path, "a") as f_itm:
        for catalog in sub_catalogs:
            collection_id = "UMBRA_" + catalog.id

            print(f"Looking for Items in {collection_id} Collection")

            spatial_extent = []
            temporal_extent = []
            collection_assets = {}

            items_links = []
            # Year
            for _, sub, _ in catalog.walk():
                # Month -> Day
                for ss in sub:
                    if not ss.get_child_links():
                        items_links.extend(
                            [link.absolute_href for link in ss.get_item_links()]
                        )

            with futures.ThreadPoolExecutor(max_workers=50) as executor:
                with click.progressbar(
                    length=len(items_links), show_percent=True, show_pos=True
                ) as bar:
                    future_to_item = [
                        executor.submit(_get_item, link) for link in items_links
                    ]

                    # Loop through each items
                    # edit items and save into a top level collection JSON file
                    for future in futures.as_completed(future_to_item):
                        item_dict = future.result()
                        item_dict["links"] = []
                        item_dict["collection"] = collection_id
                        item_dict["id"] = item_dict["id"].replace("/", "_")

                        if "stac_extensions" not in item_dict:
                            item_dict["stac_extensions"]

                        if with_s3_urls:
                            item_dict["stac_extensions"].append(
                                "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"
                            )
                            for asset in item_dict["assets"]:
                                ori = item_dict["assets"][asset]["href"].replace("http://", "https://")
                                item_dict["assets"][asset]["href"] = ori.replace(
                                    "https://umbra-open-data-catalog.s3.amazonaws.com",
                                    "s3://umbra-open-data-catalog",
                                )
                                item_dict["assets"][asset]["alternate"] = {
                                    "public": {
                                        "title": "Public Access",
                                        "href": ori,
                                    }
                                }

                        if bbox := item_dict.get("bbox"):
                            spatial_extent.append(bbox)

                        st = item_dict["properties"].get("start_datetime")
                        et = item_dict["properties"].get("end_datetime")
                        if st and et:
                            temporal_extent.extend((st, et))
                        else:
                            dt = item_dict["properties"]["datetime"]
                            temporal_extent.append(dt)

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
                        bar.update(1)

                xmins, ymins, xmaxs, ymaxs = zip(*spatial_extent)
                bbox = min(xmins), min(ymins), max(xmaxs), max(ymaxs)
                dts = sorted(temporal_extent)
                start_datetime, end_datetime = dts[0], dts[-1]

                col = pystac.collection.Collection(
                    id=collection_id,
                    title=f"UMBRA OpenData for {catalog.id}",
                    description=f"UMBRA OpenData for {catalog.id}",
                    extent=pystac.Extent(
                        spatial=pystac.SpatialExtent([bbox]),
                        temporal=pystac.TemporalExtent(
                            [
                                pystac.utils.str_to_datetime(start_datetime),
                                pystac.utils.str_to_datetime(end_datetime),
                            ]
                        )
                    ),
                    stac_extensions=[
                        "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                    ],
                    license="CC-BY-4.0",
                    extra_fields={
                        "item_assets": collection_assets
                    }
                )
                c = col.to_dict()
                f_col.write(json.dumps(c) + "\n")


if __name__ == "__main__":
    main()
