sudo docker-compose up -d
sleep 1
export PGPASSWORD=onedb
psql -h localhost -p 13101 -U postgres -f ./config/test_1.sql
psql -h localhost -p 13102 -U postgres -f ./config/test_2.sql
psql -h localhost -p 13103 -U postgres -f ./config/test_3.sql