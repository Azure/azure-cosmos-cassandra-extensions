# Azure Cosmos Cassandra Extensions for DataStax Java Driver 4 for Apache Cassandra
## Release instructions

- [ ] Create a release branch based on develop/java-driver-4.
  
      ```bash
      git pull --all
      git checkout develop/java-driver-4
      git checkout -b release/java-driver-4/$version
      ```

- [ ] Update these `package` files:

      * package/CHANGELOG.md
      * package/KNOWN_ISSUES.md
      * package/README.md

      as needed.

- [ ] Bump the version numbers in:

      * pom.xml
      * examples/pom.xml
      * package/pom.xml

      if required.
      
- [ ] Push your changes and submit a release PR against the code on develop/java-driver-4.

      Use this title text:

      `[CHORE] Release Azure Cosmos Cassandra Extensions for DataStax Java Driver 4`

      Start the description with this text:

      This is release <version>.

- [ ] When your PR is complete, publish the release artifacts from the CI build to the Maven Repository.

      - [ ] Retain the build to preserve the release artifacts and a record of the test results.
  
      - [ ] Verify that the parent pom has been published to:

            https://repo.maven.apache.org/maven2/com/azure/azure-cosmos-cassandra-driver-4

      - [ ] Verify that the package has been published to:

            https://repo.maven.apache.org/maven2/com/azure/azure-cosmos-cassandra-driver-4-extensions

