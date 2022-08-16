
# prepare env
cp -r cert/local/* owner/cert
cp -r cert/local/ca.pem user/cert
cp -r cert/local/ca.pem ../release/cert
cd user
docker-compose down
cd ../owner
docker-compose down
cd ../database
docker-compose down

# set up
docker-compose up -d
cd ../owner
docker-compose up -d