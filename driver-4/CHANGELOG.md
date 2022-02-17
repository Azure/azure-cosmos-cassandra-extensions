# Release History

## 1.1.3

This is a maintenance release to address the [*Log4j2 Vulnerability “Log4Shell” (CVE-2021-44832)*][8].

### Log4j2 Vulnerability “Log4Shell” (CVE-2021-44832)

The vulnerability that this change addresses is described in the [National Vulnerabilities Database][4]. On 12/28/2021
Apache released version [Apache log4j2 2.17.1][10]. Apache Log4j2 versions 2.0-beta7 through 2.17.0 (excluding 
security fix releases 2.3.2 and 2.12.4) are vulnerable to a remote code execution (RCE) attack where an attacker with 
permission to modify the logging configuration file can construct a malicious configuration using a JDBC Appender with 
a data source referencing a JNDI URI which can execute remote code. This issue is fixed by limiting JNDI data source
names to the java protocol in Log4j2 versions 2.17.1, 2.12.4, and 2.3.2.

The product code takes no dependency on Apache Log4j2. It uses [slf4j-api][6] instead. The test code dependency on 
Apache Log4j 2.17.0 or higher has been updated to [Apache Log4j 2.17.1][5] or higher. The test code is not released with 
the product.

[Microsoft’s Response to CVE-2021-44228 Apache Log4j 2][7] includes a summary, analysis, and mitigation guidance for
addressing this vulnerability.

## 1.1.2

This is a maintenance release to address the [*Log4j2 Vulnerability “Log4Shell” (CVE-2021-45105)*][11] reported between
late November and early December 2021. 

### Log4j2 Vulnerability “Log4Shell” (CVE-2021-45105)

The vulnerability that this change addresses is described in the [National Vulnerabilities Database][4]. On 12/17/2021
Apache released version [Apache log4j2 2.17.0][9] after discovering issues with their previous release, 2.16.0, 
published on 12/14/2021. The test and example code in this repository depend on Apache log4j2 and this release bumps 
the version number for log4j2 from 2.16 to [2.17][12]. The product code takes no dependency on log4j2. It uses 
[slf4j-api][6] instead.

[Microsoft’s Response to CVE-2021-44228 Apache Log4j 2][7] includes a summary, analysis, and mitigation guidance for
addressing this vulnerability.

## 1.1.1

This is a maintenance release to address the [*Log4j2 Vulnerability “Log4Shell” (CVE-2021-45046)*][3] reported between 
late November and early December 2021 and resolve these `CosmosLoadBalancingPolicy` issues:

- Preferred regions are now properly ordered when the primary region is explicitly specified.
  Prior to this release, if the primary region was explicitly specified in the list of preferred regions, it would be
  moved to the end of the preferred region list.

- `CosmosLoadBalancingPolicy::onDown` now removes hosts based on endpoint address, not datacenter name.
  This behavior change avoids a problem that arises when a host is removed before its datacenter name has been
  determined. It also ensures that the address of the host removed matches the address of the host to be
  removed.

This release also adds test coverage to more thoroughly ensure that `CosmosLoadBalancingPolicy` orders hosts correctly
based on the specification of preferred regions.

### Log4j2 Vulnerability “Log4Shell” (CVE-2021-45046)

This vulnerability is described in the [National Vulnerabilities Database][4]. On 12/14/2021 Apache released version 
[Apache log4j2 2.16.0][13] to completely remove support for Message Lookups and disable JNDI by default. The test and 
example code in this repository depend on Apache log4j2 and this release bumps the version number for log4j2 from 2.13 
to [2.16][14]. The product code takes no dependency on log4j2. It uses [slf4j-api][6] instead.

[Microsoft’s Response to CVE-2021-44228 Apache Log4j 2][7] includes a summary, analysis, and mitigation guidance for
addressing this vulnerability.

## 1.1.0

This is the first maintenance release of the Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra.
Since 1.0.0 we've cleaned up the source, simplified the build, improved test coverage, and overhauled the examples.
Notably,

- The examples now reside on Azure-Samples and are included as submodules with integration tests. See:

  - [Azure-Samples/azure-cosmos-cassandra-extensions-java-spring-boot-sample-v4/][1]
  - [Azure-Samples/azure-cosmos-cassandra-extensions-java-sample-v4][2]

- We've added Bash and PowerShell build scripts that make building the product locally easy on Linux, macOS, and 
  Windows. See `build` and `build.ps1` in the [sources][0].

## 1.0.0

This is the first release of the Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra.
Since Beta 1 we've simplified `CosmosLoadBalancingPolicy` and fixed a few bugs.

Get started using this package by reviewing the `README.md` and--if you're a Spring Data Cassandra user, take a look at 
the `CosmosCassandraConfiguration` class. It takes a dependency on this package and is published separately at these
Maven Repository coordinates.
```xml
<dependency>
  <groupId>com.azure</groupId>
  <artifactId>azure-cosmos-cassandra-spring-data-extensions</artifactId>
  <version>1.0.0</version>
</dependency>
```
You'll find the sources [here][0].

Learn how to use `CosmosCassandraConfiguration`, `CosmosCassandraLoadBalancingPolicy`, and `CosmosCassandraRetryPolicy`
with Spring Boot or a plain ordinary Java Application by taking a look at these Azure Samples on GitHub:

- [Azure-Samples/azure-cosmos-cassandra-extensions-java-spring-boot-sample-v4/][1]
- [Azure-Samples/azure-cosmos-cassandra-extensions-java-sample-v4][2]

## 0.1.0-beta.1

This is the first preview release of the Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra. Get 
started using this package by reviewing `README.md`.

[0]: https://github.com/Azure/azure-cosmos-cassandra-extensions/tree/develop/java-driver-4
[1]: https://github.com/Azure-Samples/azure-cosmos-cassandra-extensions-java-spring-boot-sample-v4/
[2]: https://github.com/Azure-Samples/azure-cosmos-cassandra-extensions-java-sample-v4/
[3]: https://nvd.nist.gov/vuln/detail/CVE-2021-44228
[4]: https://nvd.nist.gov/
[5]: https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core/2.17.1
[6]: http://www.slf4j.org
[7]: https://msrc-blog.microsoft.com/2021/12/11/microsofts-response-to-cve-2021-44228-apache-log4j2/
[8]: https://nvd.nist.gov/vuln/detail/CVE-2021-44832
[9]: https://logging.apache.org/log4j/2.x/security.html#CVE-2021-45105
[10]: https://logging.apache.org/log4j/2.x/security.html#CVE-2021-44832
[11]: https://nvd.nist.gov/vuln/detail/CVE-2021-45105
[12]: https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core/2.17.0
[13]: https://logging.apache.org/log4j/2.x/security.html#CVE-2021-45046
[14]: https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core/2.16.0



