import os

from .geonames import GeoNames, get_admin1, get_admin2, get_cities, get_countries
from .wb import WB, get_indicators, get_topics, search, wb_get_countries
from .yahoo.misc import get_currencies, get_exchanges, get_market_summary, get_trending
from .yahoo.screener import Screener
from .yahoo.ticker import Ticker

# Directory to export values to
# NEO4J_IMPORT = "/home/dundy/Desktop/myNewApp/data/testdata"
GEONAMES_DUMP = "https://download.geonames.org/export/dump/"
GEONAMES_ZIP = "https://download.geonames.org/export/zip/"
WB_API_URL = "https://api.worldbank.org/v2"
EXPORT_DIR = "geodataimport/data/"


# Update Environment Variables
# os.environ.update({"NEO4J_IMPORT": NEO4J_IMPORT})
os.environ.update({"GEONAMES_DUMP": GEONAMES_DUMP})
os.environ.update({"GEONAMES_ZIP": GEONAMES_ZIP})
os.environ.update({"EXPORT_DIR": EXPORT_DIR})
os.environ.update({"WB_URL": WB_API_URL})

PKG = os.path.dirname(__file__)

# __version__ = get_versions()["version"]
# del get_versions
