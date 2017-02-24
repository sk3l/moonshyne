#!/bin/bash

CONTAINER=moonshyne_pgdb

docker exec -it $CONTAINER psql -U postgres -h 127.0.0.1
