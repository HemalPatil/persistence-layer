#! /bin/bash

echo $0
echo $1
exit

export PGPASSWORD='testpassword'
sudo -i -u postgres createdb -O greennav greennavdb
sudo -i -u postgres psql -c "CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology; CREATE EXTENSION hstore; CREATE EXTENSION postgis_sfcgal" greennavdb
osm2pgsql -c -d lolax -U greennav $1 -H localhost --slim --hstore-all -l

sudo -i -u postgres psql -c "ALTER TABLE planet_osm_nodes ADD COLUMN way geometry(Point, 4326);"
sudo -i -u postgres psql -c "UPDATE planet_osm_nodes SET way=st_setsrid(st_makepoint((CAST lon AS double precision)/10000000, (CAST lat AS double precision)/10000000), 4326);"