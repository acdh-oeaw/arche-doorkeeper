#!/bin/bash
echo "listen_addresses = '*'" >> /home/www-data/postgresql/postgresql.conf
sed -i -E 's/peer|ident|md5/trust/g' /home/www-data/postgresql/pg_hba.conf
echo "host all all 127.0.0.1/0 trust" >> /home/www-data/postgresql/pg_hba.conf
echo "host all all ::1/0 trust" >> /home/www-data/postgresql/pg_hba.conf
