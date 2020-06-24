#!/bin/bash
echo "DELETE FROM metadata WHERE property = 'https://vocabs.acdh.oeaw.ac.at/schema#vocabs' AND value ~ 'iso|oefos';" | psql
