#!/bin/bash

set -e
#set -x

source /stratio/b-log.sh
source /stratio/kms_utils.sh

## VAULT LOGIN =====================================================

DOCKER_LOG_LEVEL=${DOCKER_LOG_LEVEL:-DEBUG}
eval LOG_LEVEL_${DOCKER_LOG_LEVEL}
B_LOG --stdout true # enable logging over stdout

export PORT0=${PORT0:-"8080"}

declare -a VAULT_HOSTS
IFS_OLD=$IFS
IFS=',' read -r -a VAULT_HOSTS <<< "$VAULT_HOST"

declare -a MARATHON_ARRAY
OLD_IFS=$IFS
IFS='/' read -r -a MARATHON_ARRAY <<< "$MARATHON_APP_ID"
IFS=$OLD_IFS

INFO "Trying to login in Vault"
# Approle login from role_id, secret_id
if [ "xxx$VAULT_TOKEN" == "xxx" ];
then
    INFO "Login in vault..."
    login
    if [[ ${code} -ne 0 ]];
    then
        ERROR "  - Something went wrong log in in vault. Exiting..."
        return ${code}
    fi
fi
INFO "  - Logged!"

export JAVA_ARGS="--server.port=${PORT0}"


## POSTGRES DB =====================================================

INFO "Postgres connection config start"


export POSTGRES_CERT="/etc/stratio/${MARATHON_SERVICE_NAME}.pem"
export POSTGRES_KEY="/etc/stratio/${MARATHON_SERVICE_NAME}.key"
export CA_BUNDLE_PEM="/etc/stratio/ca-bundle.pem"

INFO "  - Postgres certificate path ${POSTGRES_CERT}"
INFO "  - Retreiving service certificate [PEM] from url: /${MARATHON_SERVICE_NAME} "
getCert "userland" \
            "${MARATHON_SERVICE_NAME}" \
            "${MARATHON_SERVICE_NAME}" \
            "PEM" \
            "/etc/stratio" \
&& INFO "    - OK" \
|| INFO "    - Error"

INFO "  - Getting Ca-Bundle for a given SSL_CERT_PATH/ca-bundle.pem"
getCAbundle "/etc/stratio" "PEM" \
    && INFO "    - OK:."   \
    || INFO "    - Error."


## RUN PYTHON APP ===================================================

# TODO: PUT MAIN HERE
python3 /app/api_controller.py