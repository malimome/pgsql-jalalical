#!/bin/bash
echo "I am installing 3 types: pdate, ptimestamp, ptimestamptz to your PGSQL installation"
echo "Make sure Postgresql server is running"

INSTALL_PATH = /usr/local/pdate
mkdir $INSTALL_PATH

cp pdate.so $INSTALL_PATH
rm -f pdate.sql
sed -e "s:_OBJWD_:$INSTALL_PATH:g" < pdate.source > pdate.sql
mv pdate.sql $INSTALL_PATH
psql -f $INSTALL_PATH/pdate.sql

echo "New Types added to PGSQL: pdate, ptimestamp, ptimestamptz"
echo "Almost all date/time functions defined in standard SQL92, are implemented."
