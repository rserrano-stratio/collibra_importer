# collibra_importer
Collibra Importer Microservice

Run in local:
docker build -t collibra-manager-local -f Dockerfile_local .
docker run --name local-postgres --network host -e POSTGRES_DB=collibra_importer -e POSTGRES_PASSWORD=mysecretpassword -d postgres
docker run --rm --name collibra-manager --network host collibra-manager-local


Production version:
docker build -t collibra-manager .
docker save collibra-manager:latest | gzip > collibra_v1.tgz


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