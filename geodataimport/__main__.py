# from geodataimport.downloadGeoData import download_files
from .wb import WB

if __name__ == "__main__":
    # download_files()
    wb = WB()
    wb.get_indicators()
    wb.get_countries()
