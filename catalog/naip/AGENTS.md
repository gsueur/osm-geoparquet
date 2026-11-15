# NAIP aerial imagery: agent guide

A guide to reading NAIP imagery in place from Microsoft Planetary Computer.
It is written by Geomermaids because the collection ships no AGENTS.md. The
imagery, the STAC collection and the hosting are Microsoft's and the USDA's:
nothing here is copied or rehosted.

- STAC collection: https://planetarycomputer.microsoft.com/api/stac/v1/collections/naip
- STAC search (API): https://planetarycomputer.microsoft.com/api/stac/v1/search
- Program: https://www.fsa.usda.gov/programs-and-services/aerial-photography/imagery-programs/naip-imagery/

## What it is

The USDA Farm Service Agency's National Agriculture Imagery Program: leaf-on
aerial imagery of the conterminous US, four bands (red, green, blue, near
infrared), 0.3 to 1 m depending on the year and state, each state flown about
every two or three years. The collection runs from 2010 to 2025. One item
per quarter quadrangle (3.75 by 3.75 minutes, about 7 by 5 km), one
Cloud-Optimized GeoTIFF each (`image` asset), in the UTM zone of the quad
(NAD83). Item properties: `naip:year`, `naip:state`, `gsd`, `datetime`
(flight date).

## Rules

1. **Search, do not list.** The collection is an API, not a static catalog:
   find items with STAC search on `bbox` (lon/lat) and a time range, newest
   first. Filter on `naip:year` to get one complete flight of a state rather
   than a patchwork of years.

   ```sh
   curl 'https://planetarycomputer.microsoft.com/api/stac/v1/search?collections=naip&bbox=-71.1,42.3,-71.0,42.4&sortby=-datetime&limit=10'
   ```

2. **Sign the asset URLs.** The files are on Azure Blob Storage and need a
   short-lived SAS token, free and anonymous: one GET per collection, valid
   about an hour.

   ```sh
   curl https://planetarycomputer.microsoft.com/api/sas/v1/token/naip
   # {"msft:expiry": "...", "token": "st=...&se=...&sig=..."}
   ```

   Append `?<token>` to each `image` href. The Python package
   `planetary-computer` does this (`planetary_computer.sign(item)`).

3. **Read windows.** Files run to hundreds of MB; a COG reader fetches only the
   blocks a window needs:

   ```python
   import planetary_computer, pystac_client, rasterio
   from rasterio.warp import transform_bounds
   from rasterio.windows import from_bounds

   api = pystac_client.Client.open(
       "https://planetarycomputer.microsoft.com/api/stac/v1",
       modifier=planetary_computer.sign_inplace)
   bbox = (-71.07, 42.35, -71.05, 42.37)
   item = next(api.search(collections=["naip"], bbox=bbox, sortby="-datetime").items())
   with rasterio.open(item.assets["image"].href) as src:
       window = from_bounds(*transform_bounds("EPSG:4326", src.crs, *bbox), src.transform)
       rgbn = src.read(window=window)   # 4 bands, uint8
   ```

   With GDAL, the signed URL behind `/vsicurl/`:

   ```sh
   gdal_translate -projwin_srs EPSG:4326 -projwin -71.07 42.37 -71.05 42.35 \
     "/vsicurl/<image href>?<token>" boston_naip.tif
   ```

4. **Keep the source grid.** Quads are in their UTM zone; mosaic or reproject
   only when needed, it resamples.

## Licence and credit

NAIP is a USDA work, in the US public domain. Credit "USDA Farm Service
Agency, National Agriculture Imagery Program (NAIP)". Planetary Computer's
terms of use apply to its API and storage:
https://planetarycomputer.microsoft.com/terms-of-use

## Elsewhere

The same imagery up to 2022 is on AWS (`s3://naip-analytic`, requester-pays;
STAC at Earth Search). USGS elevation (3DEP) and land cover (Annual NLCD) are
in the Geomermaids catalog `usgs-rasters`.
