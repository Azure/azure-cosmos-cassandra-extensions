# Azure Cosmos Cassandra Extensions for DataStax Java Driver 3 for Apache Cassandra
## Release instructions

- [ ] Create a release branch based on master.
  
      ```bash
      git pull --all
      git checkout master
      git checkout -b release/java-driver-3/$version
      ```

- [ ] Update these `package` files:

      * driver-3/CHANGELOG.md
      * driver-3/KNOWN_ISSUES.md
      * driver-3/README.md

      as needed.

- [ ] Bump the version numbers in:

      * pom.xml
      * examples/pom.xml
      * package/pom.xml

      if required.
      
- [ ] Push your changes and submit a release PR against the code on master.

      Use this title text:

      `[CHORE] Release Azure Cosmos Cassandra Extensions for DataStax Java Driver 3`

      Start the description with this text:

      This is release <version>.

- [ ] When your PR is complete, publish the release artifacts from the CI build to the Maven Repository.

      Retain the build to preserve the release artifacts and a record of the test results.
