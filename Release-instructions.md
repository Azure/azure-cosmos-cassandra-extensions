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

- [ ] Bump the version numbers in:

      * pom.xml
      * examples/pom.xml
      * package/pom.xml
      
- [ ] Push your changes and submit a release PR against the code on develop/java-driver-4.

- [ ] When your PR is complete, publish the release artifacts from the CI build to the Maven Repository.