AIHABs systém pro predikci kvality vody
=======================================

.. contents:: Table of Contents
  :depth: 2

Vytvořní polygonů a náhodných bodů
----------------------------------

* body se budou vytvářet pro vnitřní polygon, který je oproti původnímu zmenšený o definovaný buffer
* Výběr vodní plochy podle osm_id
  * pro Orlík je osm_id = 11092220
* Názvy vrstev definovaný podle OSM_ID:
  * osm_id_reservoir - pro původní data
  * osm_id_buffer - pro data s odečteným bufferem pro okraj
  * osm_id_points - bodová vrstva
  * osm_id_bpoints - bodová vrstva s bufferem (polygony)
* TODO: Je otázka, jestli z funkce extrahovat gdf nebo si pak data následně načítat. Uvidíme.
* TODO: asi bude chtít info o datech ukládat do SQL databáze



TODO: Zonální statistiky
------------------------

* bude chtít přepsat
* možná GeoPandas + Rasterio
* bude pracovat s mediánem hodnot
* body budou sloupce
* rastry budou řádky --> klíč bude datum v ISO formátu, tak, aby se data dala parsovat s meteo daty.
* info o datech by bylo potřeba ukládat do SQL databáze


Načtení vrstev z Copernicus data space ecosystem
------------------------------------------------

* TODO: definovat časovou řadu a nějak rozumně jí rozdělit na části (např. po deseti dnech...):

.. code-block::

    start = datetime.datetime(2019, 1, 1)
    end = datetime.datetime(2019, 12, 31)

    n_days = (end - start).days
    n_chunks = n_days//10       # potřeba dořešit, aby se načítaly i poslední hodnoty
    print(n_chunks)
    tdelta = (end - start) / n_chunks
    print()
    edges = [(start + i * tdelta).date().isoformat() for i in range(n_chunks)]
    slots = [(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]

    print("Monthly time windows:\n")
    for slot in slots:
        print(slot)

* TODO: multithreading pro stahování?
* TODO: zapisovat stažená data do nějaké databáze - při stahování kontrola, jestli už jsou data k dispozici

TODO Databáze
-------------

Uložení dat do databáze bude zahrnovat PostgreSQL a PostGIS
* V databázi budou tabulky pro vektory
    * bude potřeba pro každý výpočet použít polygon a body počítat zvlášť, jinak v tom bude chaos

TODO: Obecné
------------

* Bude potřeba řešit procesy v rámci třídy a definovat proměnné pro celou třídu - vrtvy v paměti
    * předefinovat vstupy funkcí na dataframe a ne na načítání souborů z disku
* multithreading - může běžet preprocessing, zatímco se budou data stahovat. Asi na sebe nemusí čekat
* všechny informace je potřeba valit do SQL databáze a následně kontrolovat co má systém k dispozici a co ne
* pro měření času zpracování použít wrapper:

.. code-block::

    def measure_execution_time(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Function {func.__name__} took {execution_time:.4f} seconds to execute")
            return result
        return wrapper

    @measure_execution_time
    def func():
        pass

* pokud budou dva nebo víc znímků z jednoho termínu pro jednu lokalitu, tak bych pro další analýzy dělal mozaiku
    * část dat může chybět
    * nebude tak ztráta dat