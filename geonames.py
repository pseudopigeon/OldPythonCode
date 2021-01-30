import os
from io import BytesIO
import io
import csv
import requests
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
from pathlib import Path



NEO4J_IMPORT = Path(os.getenv('NEO4J_IMPORT'))
print(NEO4J_IMPORT)

CACHE = Path(NEO4J_IMPORT / 'cache')
CACHE.mkdir(exist_ok=True)

def import_countries():
    country_url = 'https://download.geonames.org/export/dump/countryInfo.txt'
    names = ['ISO','ISO3','ISO-Numeric','fips','Country','Capital','Area(in sq km)','Population',
         'Continent','tld','CurrencyCode','CurrencyName','Phone','Postal Code Format',
         'Postal Code Regex','Languages','geonameid','neighbours','EquivalentFipsCode'
        ]
    countries = pd.read_csv(country_url, sep='\t',comment='#', dtype='str', names=names)
    # Add missing ISO code for nambia
    index = countries.query("ISO3 == 'NAM'").index
    countries.at[index, 'ISO'] = 'NA'
    countries['id'] = countries['ISO'] # standard id column to link nodes
    countries.rename(columns={'ISO': 'iso'}, inplace=True)
    countries.rename(columns={'ISO3': 'iso3'}, inplace=True)
    countries.rename(columns={'ISO-Numeric': 'isoNumeric'}, inplace=True)
    countries.rename(columns={'Country': 'name'}, inplace=True)
    countries.rename(columns={'Population': 'population'}, inplace=True)
    countries.rename(columns={'Area(in sq km)': 'areaSqKm'}, inplace=True)
    countries.rename(columns={'geonameid': 'geonameId'}, inplace=True)
    countries.rename(columns={'Continent': 'parentId'}, inplace=True)
    countries = countries[['id','name','iso','iso3','isoNumeric', 'parentId', 'areaSqKm','geonameId', 'neighbours']].copy()
    countries.fillna('', inplace=True)
    countries.to_csv(NEO4J_IMPORT / "00e-GeoNamesCountry.csv", index=False)

def import_admin1():
    admin1_url = 'https://download.geonames.org/export/dump/admin1CodesASCII.txt'
    names = ['code', 'name', 'name_ascii', 'geonameid']
    admin1 = pd.read_csv(admin1_url, sep='\t', dtype='str', names=names)
    admin1 = admin1[['code', 'name_ascii', 'geonameid']]
    admin1.rename(columns={'code': 'id'}, inplace=True) # standard id column to link nodes
    admin1.rename(columns={'name_ascii': 'name'}, inplace=True)
    admin1.rename(columns={'geonameid': 'geonameId'}, inplace=True)
    admin1['code'] = admin1['id'].str.split('.', expand=True)[1]
    admin1['parentId'] = admin1['id'].str.split('.', expand=True)[0]
    admin1['name'] = admin1['name'].str.replace('Washington, D.C.', 'District of Columbia')
    admin1 = admin1[['id','name','code','parentId', 'geonameId']]
    admin1.fillna('', inplace=True)
    admin1.to_csv(NEO4J_IMPORT / "00f-GeoNamesAdmin1.csv", index=False)

def import_admin2():
    admin2_url = 'https://download.geonames.org/export/dump/admin2Codes.txt'
    names = ['code', 'name', 'name_ascii', 'geonameid']
    admin2 = pd.read_csv(admin2_url, sep='\t', dtype='str', names=names)
    admin2 = admin2[['code', 'name_ascii', 'geonameid']]
    admin2.rename(columns={'code': 'id'}, inplace=True) # standard id column to link nodes
    admin2.rename(columns={'name_ascii': 'name'}, inplace=True)
    admin2.rename(columns={'geonameid': 'geonameId'}, inplace=True)
    admin2['parentId'] = admin2['id'].str.rsplit('.', 1, expand=True)[0]
    admin2.loc[admin2['id'] == 'US.DC.001', 'name'] = 'District of Columbia'
    admin2.loc[admin2['id'] == 'US.CA.075', 'name'] = 'San Francisco'
    admin2.to_csv(NEO4J_IMPORT / "00g-GeoNamesAdmin2.csv", index=False)

def get_location_id(country, admin1, admin2):
    location = country
    if admin1 != '':
        location = location + '.' + admin1
    if admin2 != '':
        location = location + '.' + admin2
        
    return location

def import_cities():
    urls = ['https://download.geonames.org/export/dump/cities15000.zip', 'https://download.geonames.org/export/dump/cities5000.zip', 'https://download.geonames.org/export/dump/cities1000.zip', 'https://download.geonames.org/export/dump/cities500.zip']
    names = [
        'geonameid','name','asciiname','alternatenames','latitude','longitude','feature class',
        'feature code','country code','cc2','admin1 code','admin2 code','admin3 code','admin4 code',
        'population','elevation','dem','timezone','modification date'
    ]
    dfs = []
    for url in urls:
        file_name = url.split('/')[-1].split('.')[0] + '.txt'
        resp = urlopen(url)
        zipfile = ZipFile(BytesIO(resp.read()))
        city_df = pd.read_csv(zipfile.open(file_name), sep="\t", low_memory=False, names=names)
        dfs.append(city_df)
    
    city = pd.concat(dfs)
    city = city[['geonameid', 'asciiname', 'country code', 'admin1 code', 'admin2 code']]
    city.fillna('', inplace=True)
    city.drop_duplicates('geonameid', inplace=True)
    city.rename(columns={'geonameid': 'geonameId'}, inplace=True)
    city['id'] = city['geonameId']
    city.rename(columns={'asciiname': 'name'}, inplace=True)
    city['parentId'] = city.apply(lambda row: get_location_id(row['country code'], 
                                                            row['admin1 code'], 
                                                            row['admin2 code']), axis=1)
    city = city[['id', 'name', 'parentId', 'geonameId']]
    city.fillna('', inplace=True)
    city.to_csv(NEO4J_IMPORT / "00h-GeoNamesCity.csv", index=False)

def import_UNRegions():
    url = "https://unstats.un.org/unsd/methodology/m49/overview"
    df = pd.read_html(url, attrs={"id": "downloadTableEN"})[0]
    df.rename(columns={
                "Region Name": "UNRegion",
                "Region Code": "UNRegionCode",
                "Sub-region Name": "UNSubRegion",
                "Sub-region Code": "UNSubRegionCode",
                "Intermediate Region Name": "UNIntermediateRegion",
                "Intermediate Region Code": "UNIntermediateRegionCode",
                "ISO-alpha3 Code": "iso3",
            }, inplace=True)
    additions = pd.read_csv("/home/main/src/reference_data/UNRegionAdditions.csv")
    additions.fillna('', inplace=True)
    df = df.append(additions)
    df = df.fillna("").astype(str)
    df['UNRegionCode'] = 'm49:' + df['UNRegionCode']
    df['UNSubRegionCode'] = 'm49:' + df['UNSubRegionCode'] 
    df['UNIntermediateRegionCode'] = 'm49:' + df['UNIntermediateRegionCode']
    # Export All
    df.to_csv(NEO4J_IMPORT / "00k-UNAll.csv", index=False)
    # Export Intermediate Regions
    intermediateRegion = df[df['UNIntermediateRegion'] != '']
    intermediateRegion.to_csv(NEO4J_IMPORT / "00k-UNIntermediateRegion.csv", index=False)
    # Export Sub-regions
    subRegion = df[(df['UNSubRegion'] != '') & (df['UNIntermediateRegion'] == '')]
    subRegion.to_csv(NEO4J_IMPORT / "00k-UNSubRegion.csv", index=False)
    # Export last
    region = df[(df['UNSubRegion'] == '') & (df['UNIntermediateRegion'] == '')]
    region.to_csv(NEO4J_IMPORT / "00k-UNRegion.csv", index=False)

def add_data():
    """
    adds latitude, longitude, elevation, and population data from GeoNames 
    to Country, Admin1, Admin2, and City .csv files for ingestion into the Knowledge Graph
    """
    country_url = 'https://download.geonames.org/export/dump/allCountries.zip'
    content = requests.get(country_url)
    zf = ZipFile(BytesIO(content.content))

    for item in zf.namelist():
        print("File in zip: "+  item)
    # Intermediate data cached here
    encoding = 'utf-8'
    path = CACHE / 'allCountries.csv'
    try:
        with zf.open('allCountries.txt') as readfile:
            with open(path, "w") as file_out:
                writer = csv.writer(file_out)
                for line in io.TextIOWrapper(readfile, encoding):
                    row = line.strip().split("\t")
                    if row[6] == 'A' or row[6] == 'P':
                        writer.writerow([row[0], row[4], row[5], row[14], row[15]])
    except:
        print('Download of allCountries.txt failed, using cached version of data')
    columns = ['geonameId', 'latitude', 'longitude', 'population', 'elevation']
    # If data download failed cached file from past run is used
    df = pd.read_csv(path, names=columns, dtype='str', header=0)
    df.fillna('', inplace=True)
    df['population'] = df['population'].str.replace('0', '')
    dfc = df[['geonameId', 'latitude', 'longitude', 'population']]
    country = pd.read_csv(NEO4J_IMPORT / "00e-GeoNamesCountry.csv", dtype='str')
    country = pd.merge(country, dfc, on='geonameId', how='left')
    country.fillna('', inplace=True)
    # reset the id and iso code for Namibi.
    index = country.query("iso3 == 'NAM'").index
    country.at[index, 'iso'] = 'NA'
    country.at[index, 'id'] = 'NA'
    country.to_csv(NEO4J_IMPORT / "00e-GeoNamesCountry.csv", index=False)
    # Add data to admin1 csv 
    admin1 = pd.read_csv(NEO4J_IMPORT / "00f-GeoNamesAdmin1.csv", dtype='str')
    admin1 = pd.merge(admin1, df, on='geonameId', how='left')
    admin1.fillna('', inplace=True)
    admin1.to_csv(NEO4J_IMPORT / "00f-GeoNamesAdmin1.csv", index=False)
    # Add data for admin2 csv
    admin2 = pd.read_csv(NEO4J_IMPORT / "00g-GeoNamesAdmin2.csv", dtype='str')
    admin2 = pd.merge(admin2, df, on='geonameId', how='left')
    admin2.fillna('', inplace=True)
    admin2.to_csv(NEO4J_IMPORT / "00g-GeoNamesAdmin2.csv", index=False)
    # Add data for cities csv
    city = pd.read_csv(NEO4J_IMPORT / "00h-GeoNamesCity.csv", dtype='str')
    city = pd.merge(city, df, on='geonameId', how='left')
    city.fillna('', inplace=True)
    city.to_csv(NEO4J_IMPORT / "00h-GeoNamesCity.csv", index=False)

def import_altNames():
    url = 'https://download.geonames.org/export/dump/alternateNamesV2.zip'
    names = [
        'alternateNameId', 'geonameid', 'isolanguage', 'alternate name', 'isPreferredName',
        'isShortName', 'isColloquial', 'isHistoric', 'from', 'to'
        ]
    file_name = 'alternateNamesV2.txt'
    response = requests.get(url)
    zf = ZipFile(BytesIO(response.content))
    encoding = 'utf-8'
    path = CACHE / 'alternateNamesV2.csv'
    try:
        with zf.open('alternateNamesV2.txt') as readfile:
            with open(path, "w") as file_out:
                writer = csv.writer(file_out)
                for line in io.TextIOWrapper(readfile, encoding):
                    row = line.strip().split("\t")
                    writer.writerow(row)
    except:
        print('Download of alternateNamesV2.txt failed, using cached version of data')
    altnames = pd.read_csv(path, sep="\t", low_memory=False, names=names)
    altnames['type'] = altnames[['isPreferredName', 'shortName', 'colloquial', 'historic']].apply(lambda x: ' '.join(x), axis=1)
    altnames['type'] = altnnames['type'].apply(lambda x: filter(None, x.split(' ')))
    airport = altnames[altnames['isolanguage'].apply(lambda x: str(x) in ['iata', 'icao', 'faac'])]
    airport.rename(columns={
        'alternate name': 'name',
        'id': 'alternateNameId', 
        'parentId': 'geonameId',
        'isolanguage': 'association' 
    }, inplace=True)

def main():
    import_countries()
    import_admin1()
    import_admin2()
    import_cities()
    import_UNRegions()
    add_data()
    import_altNames()


def import_hierarchy():
    url ='hierarchy.zip'
    names = ['parentId', 'childId', 'type']
    pass

def import_languages():
    url = 'https://download.geonames.org/export/dump/iso-languagecodes.txt'

    pass

def import_featCodes():
    url ='https://download.geonames.org/export/dump/featureCodes_en.txt'
    names = ['name', 'description']
    pass

def import_timeZones():
    url = 'https://download.geonames.org/export/dump/timeZones.txt'
    names = ['timeZoneId', 'GMT_offset', 'DST_offset']
    pass


main()