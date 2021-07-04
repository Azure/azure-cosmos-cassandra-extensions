#!/usr/bin/env pwsh

using namespace System.IO

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

    mvn --file (Join-Path driver-3 pom.xml) $options -DargLine="$ArgLine" verify
    if ($LASTEXITCODE -eq 0) {
        $successes += "driver-3"
    } else {
        $failures += "driver-3"
    }

    # Tests of examples

    foreach ($app in (Get-ChildItem -Depth 0 -Directory -Path examples)) {
        mvn --file (Join-Path $app pom.xml) $options -DargLine="-Dazure.cosmos.cassandra.jar=`${project.build.directory}/`${project.build.finalName}.jar" verify
        if ($LASTEXITCODE -eq 0) {
            $successes += (Split-Path -Leaf $app)
        } else {
            $failures += (Split-Path -Leaf $app)
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
