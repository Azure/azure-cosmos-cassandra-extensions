## Release History

## 1.0.3

This is a maintenance release to address the [*Log4j2 Vulnerability “Log4Shell” (CVE-2021-44228)*][1] reported between
late November and early December 2021. 

### Log4j2 Vulnerability “Log4Shell” (CVE-2021-44228)

The vulnerability that this change addresses is described in the [National Vulnerabilities Database][2]. On 12/17/2021
Apache released version [Apache log4j2][3] 2.17.0 after discovering issues with their previous release, 2.16.0, 
published on 12/14/2021. The test and example code in this repository depend on Apache log4j2 and this release bumps 
the version number for log4j2 from 2.16 to 2.17. The product code takes no dependency on log4j2. It uses [slf4j-api][4] 
instead.

## 1.0.2

This is a maintenance release to address the [*Log4j2 Vulnerability “Log4Shell” (CVE-2021-44228)*][1] reported between
late November and early December 2021. 

### Log4j2 Vulnerability “Log4Shell” (CVE-2021-44228)

The vulnerability that this change addresses is described in the [National Vulnerabilities Database][2]. On 12/14/2021
Apache released version [Apache log4j2][3] 2.16.0 to completely remove support for Message Lookups and disable JNDI by
default. The test and example code in this repository depend on Apache log4j2 and this release bumps the version
number for log4j2 from 2.13 to 2.16. The product code takes no dependency on log4j2. It uses [slf4j-api][4] instead.

[Microsoft’s Response to CVE-2021-44228 Apache Log4j 2][5] includes a summary, analysis, and mitigation guidance for
addressing this vulnerability.

[1]: https://nvd.nist.gov/vuln/detail/CVE-2021-44228
[2]: https://nvd.nist.gov/
[3]: https://github.com/apache/logging-log4j2
[4]: http://www.slf4j.org
[5]: https://msrc-blog.microsoft.com/2021/12/11/microsofts-response-to-cve-2021-44228-apache-log4j2/

## 1.0.1

This is a maintenance release that addresses these `CosmosLoadBalancingPolicy` issues:

- Preferred regions are now properly ordered when the primary region is explicitly specified.
  Prior to this release, if the primary region was explicitly specified in the list of preferred regions, it would be 
  moved to the end of the preferred region list.

- `CosmosLoadBalancingPolicy::onDown` now removes hosts based on endpoint address, not datacenter name.
   This behavior change avoids a problem that arises when a host is removed before its datacenter name has been
   determined. It also ensures that the address of the host removed matches the address of the host to be
   removed.

- Writes now fail over to secondary regions when the primary region is down and multi-region-writes are disabled.
  The primary region is always first in the list of preferred regions for writes, but if the primary goes down, 
  writes will fail over to a secondary. Before this change, failover behavior depended entirely on account failover 
  configuration. 

This release also adds test coverage to more thoroughly ensure that `CosmosLoadBalancingPolicy` orders hosts correctly 
based on the specification of preferred regions.

## 1.0.0

This is a maintenance release that:

* Adds debug logging to `CosmosLoadBalancingPolicy`.
  
  We now log the hosts that the load balancing policy offers as they're offered. This is useful for debugging routing 
  issues.

* Renames the parent pom for azure-cosmos-cassandra-extensions as azure-cosmos-cassandra-driver-3.

  This is to trace the extensions for datastax-java-driver-3 back to that version of the driver. This is in anticipation
  of forthcoming extensions for datastax-java-driver-4. It's just the parent pom name that is changed in this release, 
  not the extensions package name. This is to avoid breaking existing dependencies. You should not need to change 
  anything other than the version number to take a dependency on this release.
  
## 0.13.0

This is a maintenance release that:

* Resolves some bugs.
* Improves the test code.
* Adds support for Azure Pipelines to build, test, and subsequently publish the package.
* Tidies the product and test code for compliance with with Azure Central SDK guidelines and Java Community standards.
