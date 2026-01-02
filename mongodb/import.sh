#!/bin/bash

echo "Importing data into MongoDB..."
mongoimport --db litcovid_json --collection bio_c_json --file ../data/mongo/litcovid2BioCJSON-converted --jsonArray
echo "Data imported successfully!"