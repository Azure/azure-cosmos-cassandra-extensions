# Azure Cosmos Cassandra Extensions for DataStax Java Driver 4 for Apache Cassandra
## Release instructions

- [ ] Create a release branch based on develop/java-driver-4.
  
  ```bash
  git checkout develop/java-driver-4
  git pull --all
  git checkout -b release/java-driver-4/${version}
  ```

- [ ] Ensure that you are on the develop branch on each of the examples
 
  ```bash
  cd examples/java-driver-app
  git checkout develop
  cd examples/spring-boot-app
  git checkout develop
  ```

- [ ] Update these files:

  * driver-4/CHANGELOG.md
  * driver-4/KNOWN_ISSUES.md
  * driver-4/README.md
  * spring-data/CHANGELOG.md
  * spring-data/KNOWN_ISSUES.md
  * spring/README.md
  * examples/java-driver-app/README.md
  * examples/spring-boot-app/README.md

  as needed. Make sure that all version numbers and all links match the release.

- [ ] Bump the version numbers in:

  * Release-instructions.md
  * pom.xml
  * driver-4/pom.xml
  * spring-data/pom.xml
  * examples/java-driver-app/pom.xml
  * examples/spring-boot-app/pom.xml

  as required.
      
- [ ] Push your changes and submit a release PR against the code on develop/java-driver-4.

  Use this title text:d
  
  ```text
  [CHORE] Release Azure Cosmos Cassandra Extensions for DataStax Java Driver 4
  ```
  
  Start the description with this text:

  ```text
  This is release <version>.
  ```
  
  Then document any other noteworthy changes. Example: `In addition to preparing for the release, the build pipeline and
  release instructions were also updated.`
  
- [ ] When your PR is complete, publish the release artifacts from the CI build to the Maven Repository.

  - [ ] Retain the build to preserve the release artifacts and a record of the test results.
  
  - [ ] Verify that the parent pom has been published to:

    https://repo.maven.apache.org/maven2/com/azure/azure-cosmos-cassandra-driver-4

  - [ ] Verify that the driver-4 extensions package has been published to:

    https://repo.maven.apache.org/maven2/com/azure/azure-cosmos-cassandra-driver-4-extensions

  - [ ] Verify that the spring-data extensions package has been published to:

    https://repo.maven.apache.org/maven2/com/azure/azure-cosmos-cassandra-spring-data-extensions

- [ ] Tag the release on the release and examples' develop branches.

  ```bash
  git tag java-driver-4/${version}
  cd examples/java-driver-app
  git tag java-driver-4/${version}
  cd ../examples/spring-boot-app
  git tag java-driver-4/${version}
  ```
  
  - [ ] Merge the examples` develop branch to main
