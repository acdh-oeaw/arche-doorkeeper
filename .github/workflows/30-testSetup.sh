#!/bin/bash
echo "DELETE FROM metadata WHERE property = 'https://vocabs.acdh.oeaw.ac.at/schema#vocabs' AND value ~ 'iso|oefos';" | psql

rm -fR /home/www-data/vendor/acdh-oeaw/arche-doorkeeper
ln -s /home/www-data/arche-doorkeeper /home/www-data/vendor/acdh-oeaw/arche-doorkeeper

CMD=/home/www-data/vendor/zozlak/yaml-merge/bin/yaml-edit.php
HD=/home/www-data
rm -f /home/www-data/vendor/acdh-oeaw/arche-doorkeeper/config.yaml
$CMD --src $HD/config/yaml/config-repo.yaml --src $HD/config/initScripts/config.yaml --srcPath '$.auth' --targetPath '$.auth' $HD/vendor/acdh-oeaw/arche-doorkeeper/config.yaml
