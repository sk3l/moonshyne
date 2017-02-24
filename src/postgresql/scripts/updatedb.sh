#!/bin/bash
SOURCE=/home/mskelton/Code/sk3l/repos/moonshyne/src/postgresql
TARGET=/home/mskelton/Code/sk3l/docker/vols/ms

cp $SOURCE/sql/moonshynedb.sql $TARGET
cp $SOURCE/sql/data_init.sql   $TARGET

docker exec moonshyne_pgdb psql -U postgres -h localhost -f /code/moonshynedb.sql
docker exec moonshyne_pgdb psql -U postgres -h localhost -f /code/data_init.sql
