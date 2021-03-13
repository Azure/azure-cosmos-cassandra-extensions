# Known issues

This file describes known issues in Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra version 0.0.1-beta.1.

## Hostname verification fails when accessing a multi-region Cosmos Cassandra API instance.

You must disable hostname verification when accessing a multi-region Cosmos Cassandra API instance with this package. Code that uses raw TLS without doing a server identity check is insecure. It allows man-in-the-middle (MITM) attacks by anyone who can obtain a certificate from a trusted root CA. 

We will address this issue before exiting beta. Until then exercise caution when hostname verification is turned off. We provide a mitigation here.

### Mitigation

Disable hostname verification and use a custom trust store that contains only the certificates you need to run your app. This would include the certificate for your multi-region Cosmos Cassandra API instance.

Do this by setting these values in your `application.conf`:
```yml
datastax-java-driver.advanced.ssl-engine-factory.hostname-verifcation = false
datastax-java-driver.advanced.ssl-engine-factory.truststore-path = <truststore-path>
datastax-java-driver.advanced.ssl-engine-factory.truststore-password = <truststore-password>
```
or from the java command line using these options:
```bash
-Ddatastax-java-driver.advanced.ssl-engine-factory.hostname-verifcation=false
-Dadvanced.ssl-engine-factory.truststore-path=$truststore_path
-Dadvanced.ssl-engine-factory.truststore-password=$truststore_password
```

Here is a bash script for importing an SSL certificate for any web service into a a `java` truststore.
```bash
#!/usr/bin/env bash

set -o errexit -o nounset

###########
# Variables
###########

declare -r script_root="$(cd "$(dirname "$0")" && pwd)"
declare -r script_name="$(basename "$0")"

# Update these variables to your liking

declare -r cacerts_storepath=~/.config/truststore.p12
declare -r cacerts_storepass=unprotected
declare -r cacerts_storetype=pkcs12

###########
# Functions
###########

function usage {

    man "${script_name}"
    exit 0
}

function error {
    echo "${script_name} error: $2" 1>&2
    exit $1
}

function note {
    echo "[$(date --iso-8601=seconds)] ${script_name} note: $1" 1>&2
}

[[ $OSTYPE != cygwin ]] || error 1 "cygwin is unsupported"

###########
# Arguments
###########

declare -r args=$(getopt --name "$script_name" --options "h" --longoptions "help,host:,name:,port:" -- $* || echo exit)
eval set -- "$args"

while [[ $1 != '--' ]]; do
    case $1 in
    -h|--help)
        usage; # does not return
        shift 1
        ;;
    --host)
        declare -r host="$2"
        shift 2
        ;;
    --name)
        declare -r name="$2"
        shift 2
        ;;
    --port)
        declare -r port="$2"
        shift 2
        ;;
  esac

done

[[ ! -z ${host:-} ]] || error 1 "value for host is required"
[[ ! -z ${port:-} ]] || declare -r port="8081"
[[ ! -z ${name:-} ]] || declare -r name="https://$host:$port/"

###########
# Main
###########

declare -r certificate_path="${TMPDIR:-${TMP:-/tmp}}/$host-$port.cer"
true | openssl s_client -connect "$host:$port" | openssl x509 > "$certificate_path"
openssl x509 -in $certificate_path -text

if [[ -f "$cacerts_storepath" ]]; then
    keytool -noprompt -keystore "$cacerts_storepath" -storepass $cacerts_storepass -alias "$name" -delete || true
fi

keytool -noprompt -keystore "$cacerts_storepath" -storepass $cacerts_storepass -storetype $cacerts_storetype -alias "$name" -importcert -file "$certificate_path"
rm "$certificate_path"

keytool -list -storepass "$cacerts_storepass" -storetype "$cacerts_storetype" -keystore "$cacerts_storepath"
```