## Release History

## 1.0.1

This is a maintenance release that addresses these `CosmosLoadBalancingPolicy` issues:

- Preferred regions are now properly ordered when the primary region is explicitly specified.
  Prior to this release, if the primary region was specified in the list of preferred regions, it would be moved to
  the end of the preferred region list.

- `CosmosLoadBalancingPolicy::onDown` now removes hosts based on endpoint address, not datacenter name.
   This behavior change avoids a problem that arises when a host is removed before its datacenter name has been
   determined. It also ensures that the address of the host removed matches the address of the host to be
   removed.

- Writes now fail over to secondary regions when the primary region is down when multi-region-writes are disabled.
  The primary region is always first in the list of preferred regions for writes, but if the primary region goes down, 
  writes will pick up on a secondary region in order as specified by `CosmosLoadBalancingPolic::preferedRegions`.

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
