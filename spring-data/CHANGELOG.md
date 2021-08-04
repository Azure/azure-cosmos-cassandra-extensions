# Release History

## Unreleased

This is the first maintenance release of the Azure Cosmos Extensions for Spring Data for Apache Cassandra. Since 
1.0.0 we've cleaned up the source, simplified the build, improved test coverage, and overhauled the
examples. Notably,

- The examples now reside on Azure-Samples and are included as submodules with integration tests. See:

    - [Azure-Samples/azure-cosmos-cassandra-extensions-java-spring-boot-sample-v4/][1]
    - [Azure-Samples/azure-cosmos-cassandra-extensions-java-sample-v4][2]

- We've added Bash and PowerShell build scripts that make building the product locally easy on Linux, macOS, and
  Windows. See `build` and `build.ps1` in the [sources][0].
  
## 1.0.0

This is the first release of the Azure Cosmos Extensions for Spring Data for Apache Cassandra. It requires:

- Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra (1.0.0)
- [DataStax Java Driver (4.7+)](http://docs.datastax.com/en/developer/java-driver/latest/)
- [Spring Data for Apache Cassandra Core (3.2+)](https://spring.io/projects/spring-data-cassandra)
  
Get started using the package by reviewing `README.md`. You'll find the sources [here][0]. Learn how to use `CosmosCassandraConfiguration`, `CosmosCassandraLoadBalancingPolicy`, and `CosmosCassandraRetryPolicy`
with Spring Boot or a plain ordinary Java Application by taking a look at these Azure Samples on GitHub:

[0]: https://github.com/Azure/azure-cosmos-cassandra-extensions/tree/develop/java-driver-4
[1]: https://github.com/Azure-Samples/azure-cosmos-cassandra-extensions-java-spring-boot-sample-v4/
[2]: https://github.com/Azure-Samples/azure-cosmos-cassandra-extensions-java-sample-v4/