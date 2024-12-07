#!/bin/bash

DBHOST="localhost" \
DBPORT="5432" \
DBNAME="postgres" \
DBUSER="postgres" \
DBPASSWD="postgres" && \
echo sudo echo "$DBHOST:$DBPORT:$DBNAME:$DBNAME:$DBNAME" > ~/.pgpass && \
cat ~/.pgpass
