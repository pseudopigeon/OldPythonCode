import warnings

import numpy as np
import pandas as pd

from geodataimport.base import _GeoData
from geodataimport.compat import lrange, reduce, string_types
from geodataimport.utils import _raise_country_error, collapse
from geodataimport.utils.config import _CONFIG
from geodataimport.utils.countries import country_codes

WB_API_URL = "https://api.worldbank.org/v2"
_cached_series = None


class WB(_GeoData):

    _format = "json"
    _wb_CONFIG = _CONFIG["wb"]

    def __init__(
        self,
        symbols=None,
        countries=None,
        start=None,
        end=None,
        freq=None,
        retry_count=3,
        pause=0.1,
        session=None,
        errors="warn",
        **kwargs,
    ):
        if symbols is None:
            symbols = ["NY.GDP.MKTP.CD", "NY.GNS.ICTR.ZS"]
        elif isinstance(symbols, string_types):
            symbols = [symbols]

        super(WB, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

        if countries is None:
            countries = ["MX", "CA", "US"]
        elif isinstance(countries, string_types):
            countries = [countries]

        bad_countries = np.setdiff1d(countries, country_codes)
        _raise_country_error(bad_countries, errors)

        freq_symbols = ["M", "Q", "A", None]

        if freq not in freq_symbols:
            msg = "The frequency `{0}` is not in the accepted " "list.".format(freq)
            raise ValueError(msg)

        self.freq = freq
        self.countries = countries
        self.errors = errors

    @property
    def url(self):
        """API URL"""
        countries = collapse(self.countries)
        return WB_API_URL + "/countries/" + countries + "/indicators/"

    @property
    def params(self):
        """Parameters to use in API calls"""
        if self.freq == "M":
            return {
                "date": "{0}M{1:02d}:{2}M{3:02d}".format(
                    self.start.year, self.start.month, self.end.year, self.end.month
                ),
                "per_page": 25000,
                "format": "json",
            }
        elif self.freq == "Q":
            return {
                "date": "{0}Q{1}:{2}Q{3}".format(
                    self.start.year, self.start.quarter, self.end.year, self.end.quarter
                ),
                "per_page": 25000,
                "format": "json",
            }
        else:
            return {
                "date": "{0}:{1}".format(self.start.year, self.end.year),
                "per_page": 25000,
                "format": "json",
            }

    def read(self):
        """Read data"""
        try:
            return self._read()
        finally:
            self.close()

    def _read_multiple(self):
        pass

    def _read(self):
        data = []
        for indicator in self.symbols:
            # Build URL for api call
            try:
                df = self._read_one_data(self.url + indicator, self.params)
                df.columns = ["country", "iso_code", "year", indicator]
                data.append(df)

            except ValueError as e:
                msg = str(e) + " Indicator: " + indicator
                if self.errors == "raise":
                    raise ValueError(msg)
                elif self.errors == "warn":
                    warnings.warn(msg)

        # Confirm we actually got some data, and build Dataframe
        if len(data) > 0:
            out = reduce(lambda x, y: x.merge(y, how="outer"), data)
            out = out.drop("iso_code", axis=1)
            out = out.set_index(["country", "year"])
            out = out.apply(pd.to_numeric, errors="ignore")

            return out
        else:
            msg = "No indicators returned data."
            raise ValueError(msg)

    def _read_lines(self, out):
        # Check to see if there is a possible problem
        if self._validate_response(out[0]):
            # Parse JSON file
            data = out[1]
            country = [x["country"]["value"] for x in data]
            iso_code = [x["country"]["id"] for x in data]
            year = [x["date"] for x in data]
            value = [x["value"] for x in data]
            # Prepare output
            df = pd.DataFrame([country, iso_code, year, value]).T
            return df

    def _validate_response(self, out):
        if "message" in out.keys():
            msg = out["message"][0]
            try:
                msg = msg["key"].split() + ["\n "] + msg["value"].split()
                wb_err = " ".join(msg)
            except Exception:
                wb_err = ""
                if "key" in msg.keys():
                    wb_err = msg["key"] + "\n "
                if "value" in msg.keys():
                    wb_err += msg["value"]

            msg = "Problem with a World Bank Query \n %s." % wb_err
            raise ValueError(msg)

        if "total" in out.keys():
            if out["total"] == 0:
                msg = "No results found from world bank."
                raise ValueError(msg)

        return True

    def wb_get_countries(self):
        """Query information about countries
        Notes
        -----
        Provides information such as:
          * country code
          * region
          * income level
          * capital city
          * latitude
          * and longitude
        """
        url = WB_API_URL + "/countries/?per_page=1000&format=json"

        resp = self._get_response(url)
        data = resp.json()[1]

        data = pd.DataFrame(data)

        data.adminregion = [x["value"] for x in data.adminregion]
        data.incomeLevel = [x["value"] for x in data.incomeLevel]
        data.lendingType = [x["value"] for x in data.lendingType]
        data.region = [x["value"] for x in data.region]
        data.latitude = [float(x) if x != "" else np.nan for x in data.latitude]
        data.longitude = [float(x) if x != "" else np.nan for x in data.longitude]
        data = data.rename(columns={"id": "iso3", "iso2Code": "iso"})
        return data

    def get_indicators(self):
        """Download information about all World Bank data series"""
        global _cached_series
        if isinstance(_cached_series, pd.DataFrame):
            return _cached_series.copy()

        url = WB_API_URL + "/indicators?per_page=50000&format=json"

        resp = self._get_response(url)
        data = resp.json()[1]

        data = pd.DataFrame(data)
        # Clean fields
        data.source = [x["value"] for x in data.source]

        def encode_ascii(x):
            return x.encode("ascii", "ignore")

        data.sourceOrganization = data.sourceOrganization.apply(encode_ascii)

        # Clean topic field
        def get_value(x):
            try:
                return x["value"]
            except Exception:
                return ""

        def get_list_of_values(x):
            return [get_value(y) for y in x]

        data.topics = data.topics.apply(get_list_of_values)
        data.topics = data.topics.apply(lambda x: " ; ".join(x))

        # Clean output
        data = data.sort_values(by="id")
        data.index = pd.Index(lrange(data.shape[0]))

        # cache
        _cached_series = data.copy()
        return data

    def get_topics(self):
        url = WB_API_URL + "/topic?per_page=100&format=json"

        resp = self._get_response(url)
        data = resp.json()[1]

        data = pd.DataFrame(data)
        return data

    def search(self, string="gdp.*capi", field="name", case=False):
        """
        Search available data series from the world bank
        Parameters
        ----------
        string: string
            regular expression
        field: string
            id, name, source, sourceNote, sourceOrganization, topics
            See notes below
        case: bool
            case sensitive search?
        Notes
        -----
        The first time this function is run it will download and cache the full
        list of available series. Depending on the speed of your network
        connection, this can take time. Subsequent searches will use the cached
        copy, so they should be much faster.
        id : Data series indicator (for use with the ``indicator`` argument of
        ``WDI()``) e.g. NY.GNS.ICTR.GN.ZS"
        name: Short description of the data series
        source: Data collection project
        sourceOrganization: Data collection organization
        note:
        sourceNote:
        topics:
        """
        indicators = self.get_indicators()
        data = indicators[field]
        idx = data.str.contains(string, case=case)
        out = indicators.loc[idx].dropna()
        return out


def download(
    country=None,
    indicator=None,
    start=2003,
    end=2005,
    freq=None,
    errors="warn",
    **kwargs,
):
    """
    Download data series from the World Bank's World Development Indicators
    Parameters
    ----------
    indicator: string or list of strings
        taken from the ``id`` field in ``WDIsearch()``
    country: string or list of strings.
        ``all`` downloads data for all countries
        2 or 3 character ISO country codes select individual
        countries (e.g.``US``,``CA``) or (e.g.``USA``,``CAN``).  The codes
        can be mixed.
        The two ISO lists of countries, provided by wikipedia, are hardcoded
        into pandas as of 11/10/2014.
    start: int
        First year of the data series
    end: int
        Last year of the data series (inclusive)
    freq: str
        frequency or periodicity of the data to be retrieved (e.g. 'M' for
        monthly, 'Q' for quarterly, and 'A' for annual). None defaults to
        annual.
    errors: str {'ignore', 'warn', 'raise'}, default 'warn'
        Country codes are validated against a hardcoded list.  This controls
        the outcome of that validation, and attempts to also apply
        to the results from world bank.
        errors='raise', will raise a ValueError on a bad country code.
    kwargs:
        keywords passed to WB
    Returns
    -------
    data : DataFrame
        DataFrame with columns country, year, indicator value
    """
    return WB(
        symbols=indicator,
        countries=country,
        start=start,
        end=end,
        freq=freq,
        errors=errors,
        **kwargs,
    ).read()


def wb_get_countries(**kwargs):
    """Query information about countries
    Provides information such as:
        country code, region, income level,
        capital city, latitude, and longitude
    Parameters
    ----------
    kwargs:
        keywords passed to WB
    """
    return WB(**kwargs).wb_get_countries()


def get_indicators(**kwargs):
    """Download information about all World Bank data series
    Parameters
    ----------
    kwargs:
        keywords passed to WB
    """
    return WB(**kwargs).get_indicators()


def get_topics(**kwargs):
    """
    Download information about all World Bank topics
    Parameters
    ----------
    kwargs:
        keywords passed to WB
    """
    return WB(**kwargs).get_topics()


def search(string="gdp.*capi", field="name", case=False, **kwargs):
    """
    Search available data series from the world bank
    Parameters
    ----------
    string: string
        regular expression
    field: string
        id, name, source, sourceNote, sourceOrganization, topics. See notes
    case: bool
        case sensitive search?
    kwargs:
        keywords passed to WB
    Notes
    -----
    The first time this function is run it will download and cache the full
    list of available series. Depending on the speed of your network
    connection, this can take time. Subsequent searches will use the cached
    copy, so they should be much faster.
    id : Data series indicator (for use with the ``indicator`` argument of
    ``WDI()``) e.g. NY.GNS.ICTR.GN.ZS"
      * name: Short description of the data series
      * source: Data collection project
      * sourceOrganization: Data collection organization
      * note:
      * sourceNote:
      * topics:
    """

    return WB(**kwargs).search(string=string, field=field, case=case)
