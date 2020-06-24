#!/bin/bash

apt install -y php-xdebug

rm -fR /home/www-data/vendor/acdh-oeaw/arche-doorkeeper
ln -s /home/www-data/arche-doorkeeper /home/www-data/vendor/acdh-oeaw/arche-doorkeeper

CMD=/home/www-data/vendor/zozlak/yaml-merge/bin/yaml-edit.php
CFGD=/home/www-data/config
rm -f /home/www-data/vendor/acdh-oeaw/arche-doorkeeper/config.yaml
echo '---'
ls -l $CFGD/yaml/config-repo.yaml
echo '---'
ls -l $CFGD/initScripts/config.yaml
echo '---'
$CMD --src $CFGD/yaml/config-repo.yaml --src $CFGD/initScripts/config.yaml --srcPath '$.auth' --targetPath '$.auth' /home/www-data/vendor/acdh-oeaw/arche-doorkeeper/config.yaml
