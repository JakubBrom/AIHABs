#!/usr/bin/env python3
# -*- coding: utf-8 -*- 

#======================================================================#
# Module: multiband_zonal_stat.py
# Author: Jakub Brom
# Date: 170104
# License: 2017, University of South Bohemia in Ceske Budejovice,
#			All rights reserved. 
# Description: Skript pro vypocet zonalnich statistik (prumeru)
#				z vetsiho poctu rastrovych souboru na zaklade vektorove
#				vrstvy ve formatu ESRI shapefile. Rastrove vrstvy jsou
#				nacteny z adresare automaticky. Vysledek se uklada do
#				csv tabulky (oddelene tabelatorem), kde radky jsou
#				rastry a sloupce jsou prumery pro jednotlive polygony
#				vektorove vrstvy usporadane podle FID.
#======================================================================#
 
# Imports
import os
import sys
import glob
import tempfile
import shutil
import sqlite3
import time
import pandas as pd
import numpy as np
import warnings
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
from osgeo.gdalconst import *
from zonal_stats import zonal_stats


#-----------------------------------------------------------------------
# Functions
def rastZonal(raster, shapefile, method = 'median', utrim = None, ltrim = None):
	"""Vypocte zonalni statistiky pro shapefile a vrati array pro prumerne 
	hodnoty razene podle FID"""
	stat_read = zonal_stats(shapefile, raster, method)			# vypocet vybranych statistik
	stats = [f[method] for f in stat_read]				# vyber promenne z vystupu a jeji zapis do seznamu
	stats = np.array(stats, dtype = float)			# prevede seznam na array

	# trim data
	if utrim is not None:
		stats[stats > utrim] = np.nan
	if ltrim is not None:
		stats[stats < ltrim] = np.nan

	return stats

def no_feature(shapefile):
	"""Vrati pocet radku v shapefilu"""
	driver = ogr.GetDriverByName('ESRI Shapefile')
	dataSource = driver.Open(shapefile, 0)
	layer = dataSource.GetLayer()
	featureCount = layer.GetFeatureCount()
	return featureCount

def reproj_shp(shapefile, rast_EPSG, driver_name='ESRI Shapefile'):
	"""Testuje jestli je shodne EPSG pro shapefile a pro raster,
	podle ktereho dochazi k orezu shapefilu. Pokud je EPSG rozdilne,
	tak funkce preprojektuje shapefile do projekce shodne s rastrem."""

	# Nacte EPSG z shapefilu
	try:
		driver = ogr.GetDriverByName(driver_name)
	except IOError:
		raise IOError("Shapefile has not been readed")

	try:
		shp_ds = driver.Open(shapefile)

		layer = shp_ds.GetLayer()
		srs = layer.GetSpatialRef()

		shp_EPSG = int(srs.GetAttrValue("AUTHORITY", 1))
	except IOError:
		warnings.warn("EPSG of input shapefile has not been readed.", stacklevel=3)

	# Porovnani EPSG rastru a shapefilu
	if shp_EPSG == rast_EPSG:
		shp_transf = shapefile
	else:
		# Definice cesty vystupu
		temp_folder = tempfile.mkdtemp()
		shp_transf = os.path.join(temp_folder, 'INPUT.shp')

		# Reprojekce
		os.system("ogr2ogr -s_srs EPSG:" + str(shp_EPSG) \
				+ " -t_srs EPSG:" + str(rast_EPSG) + " -f " \
				+ "'" + driver_name + "' " + shp_transf + " " + shapefile)

	return shp_transf

def clipVectorByRaster(raster, shapefile_in):
	"""Klipne vstupni shapefile podle rozsahu rastrove vrstvy. Funkce
	vytvori soubor ESRI Shapefile a vrati cestu k vytvorenemu souboru."""

	# Nacteni rastru
	ds = gdal.Open(raster)

	# Identifikace souradnic rastru (horni levy a dolni pravy roh)
	gtransf = ds.GetGeoTransform()
	X_size = ds.RasterXSize
	Y_size = ds.RasterYSize
	x_res = gtransf[1]
	y_res = gtransf[5]
	ulx = gtransf[0]
	uly = gtransf[3]
	lrx = ulx + X_size * x_res 
	lry = uly + Y_size * y_res

	# nacteni EPSG z rastru:
	prj = ds.GetProjection()
	ds = None
								
	# Spatial Reference System
	srs=osr.SpatialReference(wkt=prj)
	if srs.IsProjected:
		rast_EPSG = int(srs.GetAttrValue("authority", 1))

	# Kontrola transformace a pripadne vytvoreni noveho shapefilu
	shapefile = reproj_shp(shapefile_in, rast_EPSG)
	
	# Orez shapefilu
	## Nazev shapefilu
	sh_dirname, sh_filename = os.path.split(shapefile)
	sh_name, ext = os.path.splitext(sh_filename)
	## Cesta vystupu - do tmp
	temp_folder = tempfile.mkdtemp()
	out_file = os.path.join(temp_folder, 'OUTPUT.shp')
	## Vlastni orez a vytvoreni souboru v TEMP
	os.system("ogr2ogr -spat " + str(ulx) + " " + str(uly) + " " \
				+ str(lrx) + " " + str(lry) + " -clipsrc spat_extent "\
				+ out_file + " " + shapefile + " " + sh_name + " -f 'ESRI Shapefile'")

	return out_file

def exportZonalToTable(shapefile, key_field, csv_path, source_folder = None, method = 'median', utrim = None, ltrim = None):
	"""Metoda vytvori SQL databazi pro zonalni statistiky z jednotlivych rastru.
	Z primarniho shapefile souboru extrahuje primarni klic, ktery
	je nasledne pouzit pro join vsech tabulek do jedne finalni tabulky.
	Tabulka s vysledky je exportovana do csv souboru."""

	if source_folder is None:
		source_folder = os.path.dirname(__file__)

	# nacteni shapefilu a vytazeni key_field sloupce
	driver = ogr.GetDriverByName('ESRI Shapefile')
	shap = driver.Open(shapefile, GA_ReadOnly)
	vlyr = shap.GetLayer()
	key_list = []

	for feature in vlyr:
		key_value = feature.GetField(key_field)
		key_list.append(int(key_value))

	vlyr.ResetReading()
	vlyr = None

	# Vytvoreni seznamu rastrovych vrstev
	file_list = glob.glob(os.path.join(source_folder, "*.tif"))
	raster_names_list = [os.path.split(i)[1] for i in file_list]
	fnames_list = [os.path.splitext(i)[0] for i in raster_names_list]

	# Nazvy sloupcu podle nazvu souboru - definice datumu snimkovani
	col_names = []
	for i in fnames_list:
		try:
			fname_split = i.split('_')
			cname_join = '_' + ''.join(fname_split[2:5])
			if cname_join == '_' or cname_join == '' or cname_join == \
					None:
				cname_join = i
			col_names.append(cname_join)
		except:
			col_names.append(i)


	# Tvorba SQLite databaze
	sqlite_file = os.path.join(source_folder, "MultizonalDB_" + method + ".sqlite")    # name of the sqlite database file

	table_name = 'prim_table'  # name of the table to be created
	field_type_int = 'INTEGER'  # column data type
	field_type_real = 'REAL'
	all_results = 'all_res'
	
	# Connecting to the database file
	try:
		os.remove(sqlite_file)
	except:
		pass
		
	conn = sqlite3.connect(sqlite_file)
	c = conn.cursor()

	# tvorba primarni tabulky s primarnim klicem
	c.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'.format(tn = table_name, nf = key_field, ft = field_type_int))
	for i in key_list:
		c.execute('INSERT INTO {tn} VALUES ({key})'.format(tn = table_name, key = i))

	# Committing changes and closing the connection to the database file
	conn.commit()


	# vypocet zonalnich statistik (prumery)
	for i in range(len(file_list)):
		# 1. clip shapefile vrstvy
		path_to_shap = clipVectorByRaster(file_list[i], shapefile)

		# 2. ziskani primarniho klice z clipnuteho souboru
		new_shap = driver.Open(path_to_shap, GA_ReadOnly)
		new_vlyr = new_shap.GetLayer()
		new_key_list = []

		for feature in new_vlyr:
			key_value = feature.GetField(key_field)
			new_key_list.append(key_value)
	
		new_vlyr.ResetReading()
		new_vlyr = None
		new_shap = None

		# 3. Vypocet zonalnich statistik a tvorba vystupni tabulky pro konkretni raster
		# Pocet polygonu v shapefilu
		no_polygons = no_feature(path_to_shap)

		zon_stat = rastZonal(file_list[i], path_to_shap, method, utrim, ltrim)

		# 4. vytvoreni SQL tabulky pro vysledky zonalnich statistik
		c.execute('CREATE TABLE {tn} ({nf} {ft}, {nf2} {ft2})'.format(tn = fnames_list[i], nf = key_field, ft = field_type_int, nf2 = col_names[i], ft2 = field_type_real))
		for j in range(len(new_key_list)):
			data = (new_key_list[j], zon_stat[j])
			c.execute('INSERT INTO {tn} VALUES (?, ?)'.format(tn = fnames_list[i]), data)

		conn.commit()

		# smazani docasnych souboru
		sh_dir, sh_name = os.path.split(path_to_shap)

		shutil.rmtree(sh_dir)

	# join vsech dat do jedne SQL tabulky - maximalni pocet sloupcu je 4096
	## join jednotlivych tabulek do tabulek s mezivysledky
	res_names_list = []
	j = 0
	x = len(fnames_list)
	for iterno in range(x//63 + 1):
		if x//63 > 0:
			niter = 63 - 1
		else:
			niter = x % 63
		if j == 0:
			y = j * 63
		else:
			y = j * 63 - 1
		result_name = 'result{no}'.format(no = str(j+1))
		res_names_list.append(result_name)
		calc_join = ['LEFT JOIN {rtn} USING({fn})'.format(rtn = fnames_list[i], fn = key_field) for i in range(y, j * 63 + niter)]
		calc_join = ' '.join(calc_join)
		c.execute('CREATE TABLE {rt} AS SELECT * FROM {tn} {calc}'.format(rt = result_name, tn = table_name, calc = calc_join))
		conn.commit()
		j = j + 1
		x = x - 63

	## join tabulek s mezivysledky do finalni tabulky
	calc_join = ['LEFT JOIN {rtn} USING({fn})'.format(rtn = i, fn = key_field) for i in res_names_list]
	calc_join = ' '.join(calc_join)
	c.execute('CREATE TABLE {rt} AS SELECT * FROM {tn} {calc}'.format(rt = all_results, tn = table_name, calc = calc_join))
	conn.commit()


	# write csv file
	## ziskani dat z databaze
	query = 'SELECT * FROM {tn}'.format(tn = all_results)
	## prevod dat z databaze do pd DataFrame
	all_res = pd.read_sql_query(query, conn)

	## odstraneni prazdnych sloupcu z DataFrame

	nan_list = np.where(all_res.sum() == 0)	# nazvy sloupcu bez hodnot
	all_clear = all_res.drop(all_res.columns[nan_list], 1)			# odstraneni sloupcu podle nan_list

	## export do csv
	all_clear.to_csv(csv_path, sep = "\t")
	
	conn.close()

	return all_clear


#=======================================================================
if __name__ == "__main__":

	t1 = time.time()

	#-----------------------------------------------------------------------
	# shap = sys.argv[1]
	# key_field = sys.argv[2]
	#
	# csv_name = sys.argv[3]
	# method = sys.argv[4]
	# source_folder = sys.argv[5]
	#
	# try:
	# 	if sys.argv[6] is not None or sys.argv[6] is not '':
	# 		utrim = float(sys.argv[6])
	# 	else:
	# 		utrim = None
	# except:
	# 	utrim = None
	# try:
	# 	if sys.argv[7] is not None or sys.argv[7] is not '':
	# 		ltrim = float(sys.argv[7])
	# 	else:
	# 		ltrim = None
	# except:
	# 	ltrim = None
	#-----------------------------------------------------------------------
	shap = "/media/jakub/Data/projekty/SURO_Zona_2023/Data/LPIS_2022/LPIS_2022_shp.shp"
	key_field = "fid"

	# method = ["mean", "median", "min", "max", "std", "count", "mode"]
	method = ["max"]
	source_folder = "/media/jakub/Data/projekty/SURO_Zona_2023/Vystup/Vystup_230920"

	utrim = None
	ltrim = None

	# Log
	log_file_path = os.path.join(source_folder,
								 "zonal_stats_log.txt")
	with open(log_file_path, "w") as f:
		f.write("Log from calculation of the zonal statistics")
		f.write(
			"\n--------------------------------------------------------------------")


	for i in method:
		print("Statistics: {met}".format(met=i))
		t_start = time.time()
		with open(log_file_path, "a") as f:
			f.write("\n\nStatistics: {met}".format(met=i))

		try:
			csv_name = "statist_{met}_Cs137_230920.csv".format(
				met=i)
			csv_path = os.path.join(source_folder, csv_name)

			exportZonalToTable(shap, key_field, csv_path, source_folder,
							   i, utrim, ltrim)
			with open(log_file_path, "a") as f:
				f.write("\n\nStatistics {met} has been "
						"calculated".format(met=i))

		except RuntimeError:
			warnings.warn("Statistics {met} has not been "
						  "calculated.".format(met=i), stacklevel=3)
			with open(log_file_path, "a") as f:
				f.write("\n\nError: Statistics {met} has not been "
						"calculted.".format(met=i))

		t_end = time.time()
		with open(log_file_path, "a") as f:
			f.write("\nTime of calculation: " + str((t_end - t_start) // 60) + " minutes and " + str(
				(t_end - t_start) % 60) + " seconds")

		print("Time of calculation: " + str((t_end - t_start) // 60) + " minutes and " + str(
				(t_end - t_start) % 60) + " seconds")


	t2 = time.time()
	print('Vypocet trval: ' + str((t2-t1)//60) + ' minut a ' + str((t2-t1)%60) + ' sekund')
