# collibra_importer
Collibra Importer Microservice

docker build -t collibra-manager -f Dockerfile_local .
docker run --rm --name collibra-manager --network host collibra-manager


Environmente Variables:
Governance:
GOV_ROOT_URL
GOV_TENANT
GOV_USER
GOV_PASSWORD
GOV_LOGIN_MODE pass, cert

SSL:
POSTGRES_CERT
POSTGRES_KEY
CA_BUNDLE_PEM

Database:
DB_URL
DB_PORT
DB_USER
DB_NAME
DB_PASSWORD
DB_AUTH_MODE password, cert