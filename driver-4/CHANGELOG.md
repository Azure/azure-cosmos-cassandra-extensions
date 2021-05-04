## Release History

## 1.0.0

This is the first customer-ready release of the Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra.
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
