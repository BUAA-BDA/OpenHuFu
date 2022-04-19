generate() {
  echo "generate key for $1 as $1.key"
  openssl genrsa -out cert/$1.key.rsa 2048
  openssl pkcs8 -topk8 -in cert/$1.key.rsa -out cert/$1.key -nocrypt
  echo "generate certificate for $1 as $1.pem"
  openssl req -new -key cert/$1.key -out cert/$1.csr -subj "/CN=localhost"
  openssl x509 -req -CA cert/ca.pem -CAkey cert/ca.key -CAcreateserial -in cert/$1.csr -out cert/$1.pem -days 3650
}

mkdir -p cert
openssl req -x509 -new -newkey rsa:2048 -nodes -keyout cert/ca.key -out cert/ca.pem -days 3650 -subj "/CN=localhost"
generate "owner1"
generate "owner2"
generate "owner3"
cp -r cert ../owner
mkdir -p ../user/cert
cp -r cert/ca.pem ../user/cert
