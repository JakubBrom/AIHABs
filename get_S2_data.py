# Download SENTINEL-2 data over a period of time for specific points.
# Contact person: Amir H. Nikfal <a.nikfal@fz-juelich.de>

import csv
import os
import logging
import json as jsonmod
from glob import glob
import geopandas as gpd
import pandas as pd
import user_inputs as inp
try:
    import requests
except ImportError:
    print("Error: module <requests> is not installed. Install it and run again.")
    exit()

def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": inp.username,
        "password": inp.password,
        "grant_type": "password",
        }
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
            )
    return r.json()["access_token"]

# TODO: definovat časovou řadu a nějak rozumně jí rozdělit na části (např. po deseti dnech...) --> multithreading pro stahování?
# TODO: zapisovat stažená data do nějaké databáze - při stahování kontrola, jestli už jsou data k dispozici
# TODO: přidat filtr pro cloud cover nebo nějakej jinej filtr?
start_date = inp.start_day
end_date = inp.end_day

# TODO: redefinovat logging
logging.basicConfig(filename='log_sentinel_download' + str(start_date) + '.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    level=logging.INFO)


gdf = gpd.read_file("vectors/11092220/11092220_reservoir.gpkg")         # načte polygon data z vektoru
gdf_bounds = gdf.bounds
aoi = "POLYGON(({minx} {miny},{minx} {maxy},{maxx} {miny},{maxx} {maxy},{minx} {miny}))'".format(minx=str(gdf_bounds.iloc[0,0]), miny=str(gdf_bounds.iloc[0,1]), maxx=str(gdf_bounds.iloc[0,2]), maxy=str(gdf_bounds.iloc[0,3]))

data_collection = "SENTINEL-2"

cloud_cover = 10


json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' "
                    f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) "
                    f"and ContentDate/Start gt {start_date}T00:00:00.000Z "
                    f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {cloud_cover}) "
                    f"and ContentDate/Start lt {end_date}T00:00:00.000Z").json()

data_list = pd.DataFrame.from_dict(json['value']) # TODO smazat

print(data_list["Name"])        # TODO smazat




access_token = get_keycloak("username", "password")
session = requests.Session()
session.headers.update({'Authorization': f'Bearer {access_token}'})
lookedup_tiles = json['value']
for var in lookedup_tiles:
    try:
        if "MSIL1C" in var['Name']:
            # logging.info("Row found: " + str(count))
            myfilename = var['Name']
            logging.info("File OK: " + myfilename)
            # mytile = myfilename.split("_")[-2]
            # foundtiles_dict[mytile] = [point['LONG'], point['LAT']]
            url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(" + var['Id'] + ")/$value"
            response = session.get(url, allow_redirects=False)
            while response.status_code in (301, 302, 303, 307):
                logging.info("response: " + str(response.status_code))
                url = response.headers['Location']
                logging.info("next line ...")
                response = session.get(url, allow_redirects=False)
                logging.info("Last line ...")
            file = session.get(url, verify=False, allow_redirects=True)
            with open(f""+var['Name']+".zip", 'wb') as p:
                p.write(file.content)
    except:
        pass
        # count = count + 1
breakpoint()
###############################################################################
# Verifying downloaded files
###############################################################################

keycloak_token = get_keycloak("username", "password")
with open('list_downloaded_files.txt', 'w') as file:
   file.write(jsonmod.dumps(foundtiles_dict))

filelist=glob("S2*.SAFE.zip")
corrupted = []
for var in filelist:
   if int(os.path.getsize(var)/1024) < 10:
      corrupted.append(var)

count = 2
for point in corrupted:
        logging.info("Currpted Row: " + str(count))
        tile_retry = point.split("_")[-2]
        if (count+3)%4 == 0:
            keycloak_token = get_keycloak("username", "password")
        mylong = foundtiles_dict[tile_retry][0]
        mylat = foundtiles_dict[tile_retry][1]
        aoi = "POINT(" + mylong + " " + mylat + ")'"
        print("Retrying to get the point:", mylong, mylat)
        json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{data_collection}' \
                            and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) and ContentDate/Start gt {start_date} \
                                and ContentDate/Start lt {end_date}").json()
        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {keycloak_token}'})
        lookedup_tiles = json['value']
        for var in lookedup_tiles:
            try:
                if "MSIL2A" in var['Name']:
                    logging.info("Currpted Row found: " + str(count))
                    myfilename = var['Name']
                    logging.info("Currpted File OK: " + myfilename)
                    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products(" + var['Id'] + ")/$value"
                    response = session.get(url, allow_redirects=False)
                    while response.status_code in (301, 302, 303, 307):
                        logging.info("Currpted response: " + str(response.status_code))
                        url = response.headers['Location']
                        logging.info("Currpted next line ...")
                        response = session.get(url, allow_redirects=False)
                        logging.info("Currpted Last line ...")
                    file = session.get(url, verify=False, allow_redirects=True)
                    with open(f""+var['Name']+".zip", 'wb') as p:
                        p.write(file.content)
            except:
                pass
        count = count + 1