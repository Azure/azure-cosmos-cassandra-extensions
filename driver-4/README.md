# [Azure Cosmos Extensions for DataStax Java Driver 4 for Apache Cassandra][0]

This package provides extensions and a `reference.conf` file for efficiently accessing a Cosmos DB Cassandra API
instance using [DataStax Java Driver 4][1]. When you take a dependency on this package, the included `reference.conf` 
file overrides some default values set by [DataStax Java Driver 4][1]. This ensures a good out-of-box experience for 
communicating with Cosmos DB. It guarantees, for example, that each of these extensions are used by default:

- `CosmosLoadBalancingPolicy` offering options for specifying read and write datacenters to route requests.
- `CosmosRetryPolicy` throttling-aware, providing options for back-offs when failures occur.
- `ConstantReconnectionPolicy` waits a constant time between each reconnection attempt.
- `PlainTextAuthProvider` supports SASL authentication using the PLAIN mechanism as required by Cosmos DB.
- `DefaultSslEngineFactory` secures traffic between the driver, and a Cosmos DB Cassandra API instance as required by
  Cosmos DB.

The `reference.conf` file is well documented. In the sources, you can find it under [`src/main/resources`][3]. For a 
general discussion, see [DataStax Java Driver 4 configuration][2]. You can add an `application.conf` file in the 
classpath (or an absolute path, or a URL) to refine the configuration. It only needs to contain the options that you 
choose to override.

See `KNOWN_ISSUES.md` for a description of known issues in this release.

[0]: https://github.com/Azure/azure-cosmos-cassandra-extensions/blob/develop/java-driver-4/driver-4/
[1]: https://docs.datastax.com/en/developer/java-driver/latest/
[2]: https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/configuration/
[3]: https://github.com/Azure/azure-cosmos-cassandra-extensions/blob/develop/java-driver-4/driver-4/src/main/resources/reference.conf
