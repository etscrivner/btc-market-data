#!/bin/sh

# List of all spreadsheets to be downloaded
SPREADSHEETS=(
  "coinbaseUSD.csv.gz"
  "krakenUSD.csv.gz"
  "localbtcUSD.csv.gz"
)

# Remove the old spreadsheets
echo "Deleting old price data..."
rm -rf  ./raw-price-data/*

# Download the latest spreadsheets
for spreadsheet in "${SPREADSHEETS[@]}"
do
    wget -P ./raw-price-data/ "http://api.bitcoincharts.com/v1/csv/$spreadsheet"
done

# Uncompress the new spreadsheets
for compressed in ./raw-price-data/*
do
    echo "Unzipping $compressed..."
    gunzip $compressed
done
