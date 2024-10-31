# Sentinel-2

* Processing level: L2A
* Source API: [Earth Search](https://earth-search.aws.element84.com/)
* Source data: [AWS Open Data Registry](https://registry.opendata.aws/sentinel-2-l2a-cogs/)
* Format: COG
* Area: Iceland
* Time range: Whole 2023

The `generate.py` script searches the Earth Search STAC API to build collections.json and items.json
It uses a bounding box of Iceland and a time range of 2023.
It removes the jp2 assets from the results to only provide COGs.
