#!/bash/bin

echo "***** Executing curl command. *****"
echo "Pass API Key as first argument."
now="$(date +'%m_%Y')"
curl -g -o electric_consumption_data/electric_consumption_$now.json -X GET "https://api.eia.gov/v2/electricity/retail-sales/data?api_key=$1&frequency=monthly&data[0]=customers&data[1]=price&data[2]=revenue&data[3]=sales&facets[stateid][]=OH&start=2023-01&end=2023-05&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
