#!/bin/bash

apt install -y php-xdebug

#echo "listen_addresses = '*'" >> /home/www-data/postgresql/postgresql.conf
#sed -i -E 's/peer|ident|md5/trust/g' /home/www-data/postgresql/pg_hba.conf
#echo "host all all 127.0.0.1/0 trust" >> /home/www-data/postgresql/pg_hba.conf
#echo "host all all ::1/0 trust" >> /home/www-data/postgresql/pg_hba.conf

rm -fR /home/www-data/vendor/acdh-oeaw/arche-doorkeeper
ln -s /home/www-data/arche-doorkeeper /home/www-data/vendor/acdh-oeaw/arche-doorkeeper

CMD=/home/www-data/vendor/zozlak/yaml-merge/bin/yaml-edit.php
CFGD=/home/www-data/config
rm -f /home/www-data/vendor/acdh-oeaw/arche-doorkeeper/config.yaml
$CMD --src $CFGD/yaml/config-repo.yaml --src $CFGD/initScripts/config.yaml --srcPath '$.auth' --targetPath '$.auth' /home/www-data/vendor/acdh-oeaw/arche-doorkeeper/config.yaml