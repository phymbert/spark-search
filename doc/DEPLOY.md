## Deploy Spark Search to The Central Repository (OSSRH)
1. Read the publish [guide](https://central.sonatype.org/publish/)
2. The original publish ticket is [OSSRH-58231](https://issues.sonatype.org/browse/OSSRH-58231)
3. Login & browse to oss Nexus [Releases](https://oss.sonatype.org/#view-repositories;releases~browsestorage~io/github/phymbert) repository
4. Configure your [GPG keys] (https://central.sonatype.org/publish/requirements/gpg/)
5. Configure your maven settings server credentials for `sonatype` server id, example:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">

  <servers>
    <server>
      <id>sonatype</id>
      <username>XXX</username>
      <password>XXXXX</password>
    </server>
  </servers>
</settings>
```

###  Test the release
```
mvn clean verify
```
> Ideally github actions might passed, examples must run successfully

### Prepare release
```
mvn release:clean release:prepare
```

### Release and deploy
```xml
mvn release:perform
```

### Repositories
Artefacts will be available in [OSSRH Releases](https://oss.sonatype.org/content/groups/public/io/github/phymbert/)
and few hours after in [maven central repo1](https://repo1.maven.org/maven2/io/github/phymbert/) and [maven search](https://search.maven.org/search?q=g:io.github.phymbert). 

### Deploy snapshots
```xml
mvn clean deploy -Pdeploy,scala-2.11
```
