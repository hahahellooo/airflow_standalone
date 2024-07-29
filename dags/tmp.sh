#!/bin/bash

CSV_PATH=$1

user="root"
password="qwer123"
database="history_db"

mysql -u"$user" -p"$password" "$database" <<EOF
-- LOAD DATA INFILE '/var/lib/mysql-files/csv.csv'
LOAD DATA INFILE '${CSV_PATH}'
INTO TABLE history_db.tmp_cmd_usage
FIELDS TERMINATED BY ','
ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
