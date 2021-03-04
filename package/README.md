# Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra

This package provides extensions and a `reference.conf` file for correctly and efficiently accessing a Cosmos DB Cassandra API instance using [DataStax Java Driver 4](https://docs.datastax.com/en/developer/java-driver/4.10/). When you take a dependency on this package, the included `reference.conf` file overrides some of the default values set by [DataStax Java Driver 4](https://docs.datastax.com/en/developer/java-driver/latest/) to ensure a good out-of-box experience for communicating with Cosmos Cassandra API instances. It ensures, for example, that these extensions are used by default:

- `CosmosLoadBalancingPolicy` provides options for specifying read and write datacenters to route requests.
- `CosmosRetryPolicy` provides options for back-offs when failures occur.
- `DefaultSslEngineFactory` secures traffic between the driver and a Cosmos DB Cassandra API instance as required by a Comos DB.

You can add an `application.conf` file in the classpath (or an absolute path, or a URL) to refine this configuration. It only needs to contain the options that you choose to override.

The `reference.conf` file includes documentation. In the sources, it can be found under `package/src/main/resources`. For a general discussion, see [DataStax Java Driver 4 configuration](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/configuration/).
