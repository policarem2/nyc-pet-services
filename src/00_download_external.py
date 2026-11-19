"""
00_download_external.py
Grabs the three outside datasets we need and drops them in data/raw/:

  osm_pet_services.csv  - vets, groomers, pet stores from OpenStreetMap (Overpass API)
  census_acs_zcta.csv   - population, households, median income by ZIP (Census ACS 5-year)
  zcta_centroids.csv    - lat/lon center + land area for each ZIP (Census Gazetteer)

Only uses the Python standard library, so no extra installs.

Usage:
    python src/00_download_external.py                 # all three
    python src/00_download_external.py --what osm      # just one (osm, census, gazetteer)

If a download fails, the error message says which URL died. The Census ones are
by year, so if the default year isn't published yet try --acs-year / --gaz-year
with an earlier one (2022 or 2023 should work).
"""

import argparse
import csv
import io
import json
import os
import time
import urllib.parse
import urllib.request
import zipfile

# Census asks for a real user agent, and Overpass is happier with one too.
USER_AGENT = "nyc-pet-services-class-project/1.0 (student project)"

# Rough box around NYC: south, west, north, east.
# Heads up: this also catches bits of NJ and Long Island. That's fine for
# "how far is the nearest vet" since people cross borders, but keep it in mind.
NYC_BBOX = (40.49, -74.27, 40.92, -73.68)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

NYC_ZIP_MIN = 10001
NYC_ZIP_MAX = 11697


def http_get(url, data=None, retries=3, timeout=300):
    """GET (or POST if data is given) with a few retries, returns raw bytes."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as e:  # network stuff fails in a lot of ways, just retry
            last_error = e
            print(f"  attempt {attempt} for {url} failed: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError(f"Gave up on {url}: {last_error}")


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows -> {path}")


def zip_in_nyc_range(z):
    try:
        return NYC_ZIP_MIN <= int(z) <= NYC_ZIP_MAX
    except (TypeError, ValueError):
        return False


# ---------- OpenStreetMap ----------

def classify_osm(tags):
    """Turn OSM tags into one of our three categories (or None)."""
    if tags.get("amenity") == "veterinary":
        return "vet"
    if tags.get("shop") == "pet_grooming":
        return "groomer"
    if tags.get("shop") == "pet":
        return "pet_store"
    return None


def parse_osm(elements):
    """Overpass elements -> list of csv rows, with exact-duplicate spots removed."""
    rows, seen = [], set()
    for el in elements:
        tags = el.get("tags", {})
        category = classify_osm(tags)
        lat = el.get("lat", el.get("center", {}).get("lat"))
        lon = el.get("lon", el.get("center", {}).get("lon"))
        if category is None or lat is None or lon is None:
            continue
        name = tags.get("name", "")
        # Same shop sometimes shows up as both a node and a building outline,
        # so drop things with the same category/name at basically the same spot.
        key = (category, name, round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        rows.append([el.get("type", ""), el.get("id", ""), category, name,
                     lat, lon, tags.get("addr:postcode", "")])
    return rows


def download_osm(out_path):
    south, west, north, east = NYC_BBOX
    bbox = f"{south},{west},{north},{east}"
    query = f"""[out:json][timeout:180];
(
  nwr["amenity"="veterinary"]({bbox});
  nwr["shop"="pet"]({bbox});
  nwr["shop"="pet_grooming"]({bbox});
);
out center tags;"""
    print("asking Overpass for NYC pet services (can take a minute)...")
    raw = http_get(OVERPASS_URL, data=urllib.parse.urlencode({"data": query}).encode())
    elements = json.loads(raw)["elements"]
    rows = parse_osm(elements)
    write_csv(out_path, ["osm_type", "osm_id", "category", "name", "lat", "lon", "postcode"], rows)
    for cat in ("vet", "groomer", "pet_store"):
        print(f"  {cat}: {sum(1 for r in rows if r[2] == cat)}")


# ---------- Census ACS ----------

def parse_census(data):
    """Census API json (list of lists, first row is the header) -> csv rows
    for NYC-range ZIPs only."""
    header, body = data[0], data[1:]
    i_pop = header.index("B01003_001E")
    i_hh = header.index("B11001_001E")
    i_inc = header.index("B19013_001E")
    i_zip = header.index("zip code tabulation area")
    rows = []
    for r in body:
        if zip_in_nyc_range(r[i_zip]):
            # Census uses big negative numbers like -666666666 for "no data".
            # Leave them in here, 02_aggregate_zip.py turns them into nulls.
            rows.append([r[i_zip], r[i_pop], r[i_hh], r[i_inc]])
    return rows


def download_census(out_path, year):
    url = (f"https://api.census.gov/data/{year}/acs/acs5"
           "?get=B01003_001E,B11001_001E,B19013_001E"
           "&for=zip%20code%20tabulation%20area:*")
    print(f"asking Census for ACS {year} 5-year by ZIP...")
    data = json.loads(http_get(url))
    rows = parse_census(data)
    write_csv(out_path, ["zip", "population", "households", "median_income"], rows)


# ---------- Gazetteer (ZIP centroids) ----------

def parse_gazetteer(text):
    """Tab-separated gazetteer text -> csv rows (zip, lat, lon, land_sqmi)."""
    reader = csv.reader(io.StringIO(text), delimiter="\t")
    # The header has stray spaces on some column names, so strip them.
    header = [h.strip() for h in next(reader)]
    i_zip = header.index("GEOID")
    i_lat = header.index("INTPTLAT")
    i_lon = header.index("INTPTLONG")
    i_area = header.index("ALAND_SQMI")
    rows = []
    for r in reader:
        if len(r) <= max(i_zip, i_lat, i_lon, i_area):
            continue
        z = r[i_zip].strip()
        if zip_in_nyc_range(z):
            rows.append([z, r[i_lat].strip(), r[i_lon].strip(), r[i_area].strip()])
    return rows


def download_gazetteer(out_path, year):
    url = (f"https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
           f"{year}_Gazetteer/{year}_Gaz_zcta_national.zip")
    print(f"downloading ZIP centroids ({year} gazetteer)...")
    raw = http_get(url)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        txt_name = [n for n in z.namelist() if n.endswith(".txt")][0]
        text = z.read(txt_name).decode("utf-8", errors="replace")
    rows = parse_gazetteer(text)
    write_csv(out_path, ["zip", "lat", "lon", "land_sqmi"], rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--what", choices=["all", "osm", "census", "gazetteer"], default="all")
    parser.add_argument("--out-dir", default="data/raw")
    parser.add_argument("--acs-year", type=int, default=2023)
    parser.add_argument("--gaz-year", type=int, default=2023)
    args = parser.parse_args()

    if args.what in ("all", "osm"):
        download_osm(os.path.join(args.out_dir, "osm_pet_services.csv"))
    if args.what in ("all", "census"):
        download_census(os.path.join(args.out_dir, "census_acs_zcta.csv"), args.acs_year)
    if args.what in ("all", "gazetteer"):
        download_gazetteer(os.path.join(args.out_dir, "zcta_centroids.csv"), args.gaz_year)


if __name__ == "__main__":
    main()
