## Release History

## 0.14.0

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
