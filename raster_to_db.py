import psycopg2
import os
import tempfile as tf


def load_raster_to_db(data, orfname, date, osm_id, loc_name, rtype, wavelength, tab_name, db_name, user):
    """
    Nahrání rasteru do databáze.

    :param data:
    :param orfname:
    :param date:
    :param osm_id:
    :param loc_name:
    :param rtype:
    :param wavelength:
    :param tab_name:
    :param db_name:
    :param user:
    :return:
    """

    # Připojení k databázi PostGIS
    conn = psycopg2.connect(
        dbname=db_name,
        user=user)

    cur = conn.cursor()

    # Vytvoření tabulky: TODO: definovat jednotlivé sloupce
    cur.execute("CREATE TABLE IF NOT EXISTS {tab_name} (id SERIAL PRIMARY KEY, "
                "rast RASTER, "
                "filename TEXT, "
                "orig_fname TEXT, "
                "date DATE, "
                "osm_id CHARACTER VARYING, "
                "loc_name CHARACTER VARYING,"
                "rtype TEXT,"
                "wavelength TEXT)".format(tab_name=tab_name))

    conn.commit()

    # Nahrání rasteru do databáze
    with tf.NamedTemporaryFile(suffix='.tif') as tmp:
        tmp_path = tmp.name
        tmp_name = os.path.basename(tmp.name)
        os.system("gdalwarp -t_srs EPSG:4326  " + data + " " + tmp_path)
        os.system("raster2pgsql -s 4326 -a -M -F {fpath} public.{tab_name} | psql -d {db_name} -U {user}".format(fpath=tmp_path, tab_name=tab_name, db_name=db_name, user=user))

    # Nahrání dat do databáze
    cur.execute("UPDATE {tab_name} SET "
                "orig_fname = '{orfname}', "
                "date = '{date}', "
                "osm_id = '{osm_id}', "
                "loc_name = '{loc_name}',"
                "rtype = '{rtype}',"
                "wavelength = '{wavelength}' "
                "WHERE filename = '{fname}'".format(tab_name=tab_name, orfname=orfname, date=date, osm_id=osm_id, loc_name=loc_name, rtype=rtype, wavelength=wavelength, fname=os.path.basename(tmp_name)))
    conn.commit()
    cur.close()
    conn.close()

    return

if __name__ == "__main__":
    db_name = "AIHABs"
    user = "jakub"
    tab_name = 'raster_tableuuu'
    data = 'data_test/b5_utm.tif'
    orfname = os.path.basename(data)
    date = "2022-05-27"
    osm_id = '11092220'
    loc_name = 'Orlik'
    rtype = 'PE'  # SR - surface reflectance, ChlA - chlorophyll A, ChlB - chlorophyll B, PCA - Phycocyanin, TSS - Total Suspended Solids, APC - Allophycocyanin, PE - Phycoerythrin, ...
    wavelength = ''

    load_raster_to_db(data, orfname, date, osm_id, loc_name, rtype, wavelength, tab_name, db_name, user)