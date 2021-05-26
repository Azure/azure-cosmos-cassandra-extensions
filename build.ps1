#!/usr/bin/env pwsh 

[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Mandatory = $false)]
    [String]
    $ArgLine = "",

    [Parameter(Mandatory = $false)]
    [FileInfo]
    $LogDirectory = (Join-Path (Resolve-Path ~) .local var log),

    [Parameter(Mandatory = $false)]
    [FileInfo]
    $ReportingDirectory = $LogDirectory
)

"-----------------------------------------------------------------------------------------------------------------------"
" B U I L D"
"-----------------------------------------------------------------------------------------------------------------------"

Get-Item -ErrorAction Stop `
    env:AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT,
    env:AZURE_COSMOS_CASSANDRA_USERNAME,
    env:AZURE_COSMOS_CASSANDRA_PASSWORD,
    env:AZURE_COSMOS_CASSANDRA_JAVA_OPTIONS,
    env:AZURE_COSMOS_CASSANDRA_MULTI_REGION_WRITES,
    env:AZURE_COSMOS_CASSANDRA_PREFERRED_REGIONS,
    env:AZURE_COSMOS_CASSANDRA_REGIONAL_ENDPOINTS,
    env:AZURE_COSMOS_CASSANDRA_TRUSTSTORE_PASSWORD,
    env:AZURE_COSMOS_CASSANDRA_TRUSTSTORE_PATH | Format-Table -AutoSize
""

git submodule update --init --recursive
mvn clean install -DskipTests

foreach ($directory in $LogDirectory, $ReportingDirectory) {
    if (!(Test-Path $directory)) {
        New-Item -Force -ItemType Directory $directory
    }
}

Get-Item -ErrorAction SilentlyContinue `
    (Join-Path $LogDirectory azure-cosmos-cassandra*.log),
    (Join-Path $ReportingDirectory *cql-requests.csv), 
    (Join-Path $ReportingDirectory *cql-messages.csv) | Remove-Item -Force -Recurse

# The numbered variables we create here are required for Typesafe Config (HOCON) which provides no mechanism for
# initializing arrays from System or Environment variables. You must add elements to a list serially like this: 
# <list-element> += <value>.

$env:AZURE_COSMOS_CASSANDRA_HOSTNAME, $env:AZURE_COSMOS_CASSANDRA_PORT = $env:AZURE_COSMOS_CASSANDRA_GLOBAL_ENDPOINT -split ':'

$i = 1

foreach ($region in $env:AZURE_COSMOS_CASSANDRA_PREFERRED_REGIONS -split ',') {
    $name = "env:AZURE_COSMOS_CASSANDRA_PREFERRED_REGION_$i"
    Set-Content -Path $name -Value $region
    $i++
}

$i = 1

foreach ($region in $env:AZURE_COSMOS_CASSANDRA_REGIONAL_ENDPOINTS -split ',') {
    $name = "env:AZURE_COSMOS_CASSANDRA_REGIONAL_ENDPOINT_$i"
    Set-Content -Path $name -Value $region
    $i++
}

""
"-----------------------------------------------------------------------------------------------------------------------"
" V E R I F Y"
"-----------------------------------------------------------------------------------------------------------------------"

function Test-Build {
    [CmdletBinding(PositionalBinding = $false)]
    param(
        [Parameter(Mandatory = $false, Position = 1)]
        [String]
        $ArgLine = ""
    )

    $options = "-Dcheckstyle.skip -Dmaven.main.skip"
    $successes = @()
    $failures = @()

    # Package tests

    mvn --file (Join-Path driver-4 pom.xml) $options -DargLine="$ArgLine" verify
    if ($LASTEXITCODE -eq 0) {
        $successes += "driver-4"
    } else {
        $failures += "driver-4"
    }

    mvn --file (Join-Path spring-data pom.xml) $options -DargLine="$ArgLine" verify
    if ($LASTEXITCODE -eq 0) {
        $successes += "spring-data"
    } else {
        $failures += "spring-data"
    }

    # Tests of examples

    foreach ($app in "java-driver-app", "spring-boot-app") {
        mvn --file (Join-Path examples $app pom.xml) $options -DargLine="-Dazure.cosmos.cassandra.jar=`${project.build.directory}/`${project.build.finalName}.jar" verify
        if ($LASTEXITCODE -eq 0) {
            $successes += (Join-Path examples $app)
        } else {
            $failures += (Join-Path examples $app)
        }
    }
    
    if ($failures.Length -eq 0) {
        ""
        "VERIFICATION SUCCESS"
    } else {
        ""
        "VERIFICATION FAILURE"
        "  SUCCEEDED: $successes"
        "  FAILED: $failures"
    }
}

Measure-Command { Test-Build $ArgLine } | Out-Default
