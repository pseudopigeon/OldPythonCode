import tempfile
import time
import warnings
from zipfile import ZipFile

from pandas import DataFrame, concat, read_csv

from geodataimport.base import _GeoData
from geodataimport.compat import StringIO, binary_type, bytes_to_str
from geodataimport.utils.config import _CONFIG

GEONAMES_URL = "https://download.geonames.org/export/dump/"
# GEONAMES_DUMP = GEONAMES_URL + "dump/"
# GEONAMES_ZIP = GEONAMES_URL + "zip/"

_config = _CONFIG["geonames"]

_delimiter = "\t"


class GeoNames(_GeoData):

    all_symbols = frozenset(
        (
            "countries",
            "cities",
            "admin1",
            "admin2",
            "coordinates",
            # "postalcodes",
            # "continents",
        )
    )

    def __init__(
        self,
        symbols=all_symbols,
        retry_count=3,
        pause=0.1,
        session=None,
        errors="warn",
        asynchronous=False,
    ):
        super(GeoNames, self).__init__(
            symbols=symbols,
            retry_count=3,
            pause=0.1,
            session=None,
            asynchronous=asynchronous,
        )
        if isinstance(self.symbols, str):
            self.symbols = [self.symbols]
        self._base_url = GEONAMES_URL
        self._format = "string"
        self.errors = errors

    @property
    def url(self):
        """
        API URL
        """
        return "https://download.geonames.org/export/dump/"

    def read(self):
        """Read data
        Returns
        -------
        data : DataFrame
            If multiple names are passed for "series" then the index of the
            DataFrame is the outer join of the indicies of each series.
        """
        try:
            return self._read()
        finally:
            self.close()

    def _read_zipfile(self, url):
        response = self._get_response(url)
        # text = self._sanitize_response(response)
        raw = response.content
        with tempfile.TemporaryFile() as tmpf:
            tmpf.write(raw)

            with ZipFile(tmpf, "r") as zf:
                text = zf.open(zf.namelist()[0]).read().decode()

        out = StringIO()
        if isinstance(text, binary_type):
            out.write(bytes_to_str(text))
        else:
            out.write(text)
        out.seek(0)
        return out

    def _read(self):
        data = []
        for location in self.symbols:
            # Build URL for api call
            config = _config[location]
            filenames = config["filenames"]
            try:
                temp = []
                for file in filenames:
                    if ".zip" in file:
                        resp = self._read_zipfile(self.url + file)
                    else:
                        resp = self._read_url_as_StringIO(self.url + file)
                    df = read_csv(
                        resp,
                        sep=_delimiter,
                        comment=config["comment"],
                        names=config["names"],
                        low_memory=config["low_memory"],
                        header=config["header"],
                    )
                    # df.columns = _config[location]["names"]
                    temp.append(df)
                df = concat(temp)
                df = self.standardize_geo_data(location, df)
                data.append(df)

            except ValueError as e:
                msg = str(e) + " Location: " + location
                if self.errors == "raise":
                    raise ValueError(msg)
                elif self.errors == "warn":
                    warnings.warn(msg)
        return data

    def get_city_location_id(self, country, admin1, admin2):
        location = str(country)
        if admin1 != "":
            location = location + "." + str(admin1)
        if admin2 != "":
            location = location + "." + str(admin2)
        return location

    def standardize_geo_data(self, location, df):
        config = _config[location]
        df.rename(columns=config["rename_cols"], inplace=True)

        if location == "admin1":
            df["code"] = df["id"].str.split(".", expand=True)[1]
            df["parentId"] = df["id"].str.split(".", expand=True)[0]
            # Change to DC to be consistent with US Census
            df["name"] = df["name"].replace("Washington, D.C.", "District of Columbia")

        elif location == "admin2":
            df["parentId"] = df["id"].str.rsplit(".", 1, expand=True)[0]
            # Change to DC and SanFran to be consistent with US Census
            df.loc[df.id == "US.DC.001", ["name"]] = "District of Columbia"
            df.loc[df.id == "US.CA.075", ["name"]] = "San Francisco"

        elif location == "countries":
            index = df.query("iso3 == 'NAM'").index  # Add missing ISO code for Nambia
            df.at[index, "iso"] = "NA"
            df["id"] = df["iso"]
            df["neighbours"] = df["neighbours"].str.split(",")
            df["languages"] = df["languages"].str.split(",")

        elif location == "cities":
            df.drop_duplicates("geonameId", inplace=True)
            df["id"] = df["geonameId"]  # Add ID Column
            df.fillna("", inplace=True)
            df["parentId"] = df.apply(
                lambda row: self.get_city_location_id(
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
        keep_cols = config["keep_cols"]
        df = df[keep_cols]
        df.fillna("", inplace=True)
        return df

    def get_continents(self):
        """Query information about continents
        Notes
        -----
        Provides information such as:
        * continent code
        * continent geonames id
        """
        data = _config["continents"]["data"]
        data = DataFrame.from_dict(data)
        return data


def download_files():
    """
    Downloads all GeoNames location files and exports them to
    local directory
    """
    start = time.time()
    data = GeoNames().read()
    end = time.time()
    total = (end - start) / 60
    print("Took {} minutes".format(total))
    return data


def get_countries():
    """
    Retrieves data for all countries from GeonNames data dump
    Notes
        -----
        Provides information such as:
         *
    """
    return GeoNames(symbols="countries").read()[0]


def get_cities():
    """
    Download data for all cities from Geonames dump
    """
    return GeoNames(symbols="cities").read()[0]


def get_admin1():
    """
    Download data for all 1st level administrative divisions from Geonames dump
    """
    return GeoNames(symbols="admin1").read()[0]


def get_admin2():
    """
    Download data for all 2nd level administrative divisions from Geonames dump
    """
    return GeoNames(symbols="admin2").read()[0]


# def search(string='USA', field="iso3", case=False, **kwargs):
