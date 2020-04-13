postgres_name="wage_trust_db"
app_name="rda_module"

echo "Building ${postgres_name}"
docker build -t "${postgres_name}" ./wage_trust_db

#echo "Building ${app_name}"
#docker build -t "${app_name}" ./rda_module