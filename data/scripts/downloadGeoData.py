import os
import time
from io import BytesIO

import pandas as pd

from geodataimport.compat import StringIO
from geodataimport.utils import (
    check_file_exists,
    dataframe_to_csv,
    load_dataframe,
    unzip_url_file,
)

DUMP_URL = os.environ["GEONAMES_DUMP"]
ZIP_URL = os.environ["GEONAMES_ZIP"]
# EXPORT_DIR = "geodataimport/data/"

_CONFIG = {
    "geonames": {
        "country": {
            "filenames": ["countryInfo.txt"],
            "comment": "#",
            "names": [
                "ISO",
                "ISO3",
                "ISO-Numeric",
                "fips",
                "Country",
                "Capital",
                "Area(in sq km)",
                "Population",
                "Continent",
                "tld",
                "CurrencyCode",
                "CurrencyName",
                "Phone",
                "Postal Code Format",
                "Postal Code Regex",
                "Languages",
                "geonameid",
                "neighbours",
                "EquivalentFipsCode",
            ],
            "low_memory": True,
            "keep_cols": [
                "id",
                "name",
                "iso",
                "iso3",
                "isoNumeric",
                "areaSqKm",
                "geonameId",
            ],
            "rename_cols": {
                "ISO": "iso",
                "ISO3": "iso3",
                "ISO-Numeric": "isoNumeric",
                "Country": "name",
                "Population": "population",
                "Area(in sq km)": "areaSqKm",
                "geonameid": "geonameId",
            },
            "header": None,
            "outfile": "GeoNames-Countries.csv",
        },
        "cities": {
            "filenames": [
                "cities15000.zip",
                "cities5000.zip",
                "cities1000.zip",
                "cities500.zip",
            ],
            "names": [
                "geonameid",
                "name",
                "asciiname",
                "alternatenames",
                "latitude",
                "longitude",
                "feature class",
                "feature code",
                "country code",
                "cc2",
                "admin1 code",
                "admin2 code",
                "admin3 code",
                "admin4 code",
                "population",
                "elevation",
                "dem",
                "timezone",
                "modification date",
            ],
            "comment": None,
            "low_memory": False,
            "keep_cols": ["id", "name", "parentId", "geonameId"],
            "rename_cols": {"geonameid": "geonameId", "asciiname": "name"},
            "header": None,
            "outfile": "GeoNames-Cities.csv",
        },
        "admin1": {
            "filenames": ["admin1CodesASCII.txt"],
            "names": ["code", "name", "name_ascii", "geonameid"],
            "comment": None,
            "low_memory": True,
            "keep_cols": ["id", "name", "code", "parentId", "geonameId"],
            "rename_cols": {
                "code": "id",
                "name_ascii": "name",
                "geonameid": "geonameId",
            },
            "header": None,
            "outfile": "GeoNames-Admin1.csv",
        },
        "admin2": {
            "filenames": ["admin2Codes.txt"],
            "names": ["code", "name", "name_ascii", "geonameid"],
            "comment": None,
            "low_memory": True,
            "keep_cols": ["id", "name", "parentId", "geonameId"],
            "rename_cols": {
                "code": "id",
                "name_ascii": "name",
                "geonameid": "geonameId",
            },
            "header": None,
            "outfile": "GeoNames-Admin2.csv",
        },
        "coordinates": {
            "filenames": ["allCountries.zip"],
            "names": [
                "geonameid",
                "name",
                "asciiname",
                "alternatenames",
                "latitude",
                "longitude",
                "feature class",
                "feature code",
                "country code",
                "cc2",
                "admin1 code",
                "admin2 code",
                "admin3 code",
                "admin4 code",
                "population",
                "elevation",
                "dem",
                "timezone",
                "modification date",
            ],
            "comment": None,
            "low_memory": False,
            "keep_cols": ["geonameId", "latitude", "longitude", "population"],
            "rename_cols": {"geonameid": "geonameId", "asciiname": "name"},
            "header": None,
            "outfile": "GeoNames-Coordinates.csv",
        },
        "postalcodes": {
            "filenames": "allCountries.zip",
            "names": [
                "country code",
                "postal code",
                "place name",
                "admin name1",
                "admin code1",
                "admin name2",
                "admin code2",
                "admin name3",
                "admin code3",
                "latitude",
                "longitude",
                "accuracy",
            ],
            "rename_cols": {"postal code": "postalCode", "place name": "placeName"},
            "keep_cols": [
                "postalCode",
                "admin2_id",
                "placeName",
                "latitude",
                "longitude",
            ],
            "comment": None,
            "low_memory": False,
            "outfile": "GeoNames-PostalCodes.csv",
            "header": 0,
        },
    },
    "continents": {
        "data": {
            "code": ["AF", "AS", "EU", "NA", "OC", "SA", "AN"],
            "name": [
                "Africa",
                "Asia",
                "Europe",
                "North America",
                "Oceania",
                "South America",
                "Antarctica",
            ],
            "geonameId": [
                "6255146",
                "6255147",
                "6255148",
                "6255149",
                "6255151",
                "6255150",
                "6255152",
            ],
        },
        "outfile": "GeoName-Continents.csv",
    },
    "un": {
        "regions": {
            "url": "https://unstats.un.org/unsd/methodology/m49/overview",
            "attrs": {"id": "downloadTableEN"},
            "rename_cols": {
                "Region Name": "UNRegion",
                "Region Code": "UNRegionCode",
                "Sub-region Name": "UNSubRegion",
                "Sub-region Code": "UNSubRegionCode",
                "Intermediate Region Name": "UNIntermediateRegion",
                "Intermediate Region Code": "UNIntermediateRegionCode",
                "ISO-alpha3 Code": "iso3",
            },
            "outfile": "UN-AllRegions.csv",
            "intermediate_outfile": "UN-IntermediateRegion.csv",
            "subregion_outfile": "UN-SubRegion.csv",
            "region_outfile": "UN-Region.csv",
        },
        "population": {
            "total": {
                "url": "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_TotalPopulationBySex.csv",
                "outfile": "UN-TotalPopulation.csv",
            },
            "age": {
                "url": "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_PopulationBySingleAgeSex_1950-2019.csv",
                "outfile": "UN-PopulationByAge.csv",
            },
        },
    },
}


def get_city_location_id(country, admin1, admin2):
    location = str(country)
    if admin1 != "":
        location = location + "." + str(admin1)
    if admin2 != "":
        location = location + "." + str(admin2)
    return location


def standardize_geo_data(location, df, config):

    df.rename(columns=config["rename_cols"], inplace=True)

    if location == "admin1":
        df["code"] = df["id"].str.split(".", expand=True)[1]
        df["parentId"] = df["id"].str.split(".", expand=True)[0]
        # Change to DC to be consistent with US Census
        print(df.head())
        df["name"] = df["name"].replace("Washington, D.C.", "District of Columbia")

    elif location == "admin2":
        df["parentId"] = df["id"].str.rsplit(".", 1, expand=True)[0]
        # Change to DC and SanFran to be consistent with US Census
        df.loc[df["id"] == "US.DC.001", ["name"]] = "District of Columbia"
        df.loc[df["id"] == "US.CA.075", ["name"]] = "San Francisco"

    elif location == "country":
        index = df.query("iso3 == 'NAM'").index  # Add missing ISO code for Nambia
        df.at[index, "iso"] = "NA"
        df["id"] = df["iso"]

    elif location == "cities":
        df.drop_duplicates("geonameId", inplace=True)
        df["id"] = df["geonameId"]  # Add ID Column
        df["parentId"] = df.apply(
            lambda row: get_city_location_id(
                row["country code"], row["admin1 code"], row["admin2 code"]
            ),
            axis=1,
        )

    elif location == "coordinates":
        df.loc[df["feature class"].isin(["A", "P"])]
        df["population"] = df["population"].str.replace("0", "")

    elif location == "postalcodes":
        df.dropna(subset=["admin code2"], inplace=True)
        df.fillna("", inplace=True)
        df["admin2_id"] = (
            df["country code"] + "." + df["admin code1"] + "." + df["admin code2"]
        )
    df = df.loc[:, config["keep_cols"]]
    df.fillna("", inplace=True)
    return df


def download_file(filename, config, url):
    file = None

    if ".zip" in filename:
        zip_file = unzip_url_file(url)
        file = zip_file.open(filename.split(".")[0] + ".txt")
    else:
        file = url

    df = pd.read_csv(
        file,
        comment=config["comment"],
        names=config["names"],
        low_memory=config["low_memory"],
        header=config["header"],
    )
    return df


def get_location_data(location):

    print("Loading %s Files..." % location)
    config = _CONFIG["geonames"][location]

    if location == "postalcodes":
        df = download_file(config["filenames"], config, ZIP_URL + config["filenames"])
    else:
        df = pd.concat(
            [
                download_file(file, config, DUMP_URL + file)
                for file in config["filenames"]
            ]
        )

    df = standardize_geo_data(location, df, config)

    return df


def merge_coordinate_data(location_df, coordinate_df):
    """ Merges Coordinate & Population Data (Latitude, Longtitude, Population) to Location Dataframes """
    df = pd.merge(location_df, coordinate_df, on="geonameId", how="left")
    df.fillna("", inplace=True)
    return df


def download_geoname_files():
    GEO_CONFIG = _CONFIG["geonames"]

    # Save Continent Data
    if not check_file_exists(_CONFIG["continents"]["outfile"]):
        continents = pd.DataFrame.from_dict(_CONFIG["continents"]["data"])
        dataframe_to_csv(continents, _CONFIG["continents"]["outfile"])

    locations = []

    for location in list(GEO_CONFIG.keys()):
        if not check_file_exists(GEO_CONFIG[location]["outfile"]):
            locations.append(location)
        else:
            print("{} File Already Downloaded, Skipping...".format(location.upper()))

    # Skip coordinates if all files are downloaded
    if locations:
        coordinate_df = get_location_data("coordinates")
        dataframe_to_csv(coordinate_df, GEO_CONFIG["coordinates"]["outfile"])

    # Download and Merge Location Files and Coordinate Data then export to CSV
    for location in locations:
        location_df = get_location_data(location)
        if location != "postalcodes":
            location_df = merge_coordinate_data(location_df, coordinate_df)
        dataframe_to_csv(location_df, GEO_CONFIG[location]["outfile"])


def download_htmlTable(config):
    df = pd.read_html(config["url"], attrs=config["attrs"])[0]
    if config["rename_cols"]:
        df.rename(columns=config["rename_cols"], inplace=True)
    df = df.fillna("").astype(str)

    # ADD M49 PREFIX FOR M49 STANDARD BY UN SECRETARIAT STATS DIVISION
    df["UNRegionCode"] = "m49:" + df["UNRegionCode"]
    df["UNSubRegionCode"] = "m49:" + df["UNSubRegionCode"]
    df["UNIntermediateRegionCode"] = "m49:" + df["UNIntermediateRegionCode"]
    return df


def export_regions_files(df, config):
    if not check_file_exists(config["intermediate_outfile"]):
        intermediateRegion = df[df["UNIntermediateRegion"] != ""]
        dataframe_to_csv(intermediateRegion, config["intermediate_outfile"])

    elif not check_file_exists(config["subregion_outfile"]):
        subRegion = df[(df["UNSubRegion"] != "") & (df["UNIntermediateRegion"] == "")]
        dataframe_to_csv(subRegion, config["subregion_outfile"])

    elif not check_file_exists(config["region_outfile"]):
        region = df[(df["UNSubRegion"] == "") & (df["UNIntermediateRegion"] == "")]
        dataframe_to_csv(region, config["region_outfile"])

    else:
        print("UN Region Files Already Downloaded, Skipping... ")


def download_UNRegions_files():
    CONFIG = _CONFIG["un"]["regions"]
    if not check_file_exists(CONFIG["outfile"]):
        df = download_htmlTable(CONFIG)
        dataframe_to_csv(df, CONFIG["outfile"])
        export_regions_files(df, CONFIG)
    else:
        print("UN Regions file already downloaded, skipping... ")


def download_UNPopulation_files():
    CONFIG = _CONFIG["un"]["population"]
    for key in CONFIG:
        outfile = CONFIG[key]["outfile"]
        if not check_file_exists(outfile):
            df = load_dataframe(CONFIG[key]["url"], low_memory=False, header=0)
            dataframe_to_csv(df, CONFIG[key]["outfile"])
        else:
            print(f"File {outfile} Already Downloaded, Skipping... ")


def download_files():
    download_geoname_files()
    download_UNRegions_files()
    download_UNPopulation_files()
