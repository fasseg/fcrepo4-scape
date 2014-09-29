SCAPE Connector API on Fedora 4 
=================================

This is the implementation of the SCAPE Connector API as described by the spec available at 
https://github.com/openplanets/scape-apis

Proxy Settings
--------------

The webapp needs access to the internet when it should fetch files from outside the local LAN or filesystem:


If you're getting 
```java 
javax.jcr.RepositoryException: java.net.ConnectException
at eu.scape_project.service.ConnectorService.addFiles(ConnectorService.java:540)
```


the Servlet Container needs to know about the proxy settings. In e.g. Tomcat you can add the following to tomcat/bin/catalina.sh:
```
CATALINA_OPTS="$CATALINA_OPTS -Dhttp.proxyHost=proxy.example.com -Dhttp.proxyPort=4242"
```

ONB specific settings
---------------------
In order to accommodate the use case of the ONB testbed ingest the following java system properties have been added:

* _scape.onb.pairtree.basepath_ The base path where the files at ONB are located
* _scape.onb.pairtree.encapsulated_ The encapsulated directory of the path

```
JAVA_OPTS="$JAVA_OPTS -Dscape.onb.pairtree.basepath=/tmp/scape/onb"
JAVA_OPTS="$JAVA_OPTS -Dscape.onb.pairtree.encapsulated=onb"
```


Managed vs. Referenced Content
------------------------------
The following java property can be used to configure the conenctor api to use managed/referenced content:
* _scape.fcrepo.content.referenced_ [Default: false] Boolean value for switching between managed and referenced content
```Java
JAVA_OPTS="$JAVA_OPTS -Dscape.fcrepo.content.referenced=true"
```
 

Prepackaged WAR 
---------------

A prepackaged Fedora 4 WAR file which includes the SCAPE Connector API and the SCAPE Plan Management API is available at

[SCAPE Fcrepo4 BETA 4 War](https://drive.google.com/file/d/0B5nd_qlYdcqySUMxeU1HVVlvUms/edit?usp=sharing)


#### Ingest an Intellectual Entity:

```bash
$ curl -X POST http://localhost:8080/fcrepo/rest/scape/entity -d @${CONNECTOR_FOLDER}/src/test/resources/entity-minimal.xml
```

#### Ingest an Intellectual Entity asynchronously:

```bash
$ curl -X POST http://localhost:8080/fcrepo/rest/scape/entity-async -d @${CONNECTOR_FOLDER}/src/test/resources/entity-minimal.xml
```

#### Retrieve an Intellectual Entity:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/entity/entity-1
```

#### Retrieve an distinct Intellectual Entity version:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/entity/entity-1/1
```

#### Retrieve an Intellectual Entity using references for child elements:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/entity/entity-1?useReferences=yes
```

#### Retrieve an Intellectual Entity list

```bash
$ curl -H "Content-Type:text/uri-list" -X POST http://localhost:8080/fcrepo/rest/scape/entity-list -d "http://localhost:8080/fcrepo/rest/scape/entity/entity-1"
```

#### Retrieve a Representation:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/representation/entity-1/representation-1
```

#### Retrieve a binary File:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/file/entity-1/representation-1/file-1
```

#### Retrieve a Bitstream:

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/bitstream/entity-1/representation-1/file-1/bitstream-1
```

#### Retrieve the life cycle state of an Intellectual Entity

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/lifecycle/entity-1
```

#### Retrieve the descriptive metadata of an Intellectual entity

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/DESCRIPTIVE
```

#### Retrieve the source/rights/provenance or technical metadata of a Representation 

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/SOURCE
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/RIGHTS
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/PROVENANCE
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/TECHNICAL
```

#### Retrieve the technical metadata of a File 

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/file-1/TECHNICAL
```

#### Retrieve the technical metadata of a BitStream 

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/file-1/bitstream-1/TECHNICAL
```

#### Retrieve the Version List of an Intellectual Entity

```bash
$ curl -X GET http://localhost:8080/fcrepo/rest/scape/entity-version-list/entity-1
```
#### Update an Intellectual Entity

```bash
$ curl -H "Content-Type:text/xml" -X PUT http://localhost:8080/fcrepo/rest/scape/entity/entity-1 -d @${CONNECTOR_FOLDER}/src/test/resources/entity-minimal.xml
```

#### Update Metadata of an Intellectual Entity/Representation/File/Bitstream

```bash
$ curl -H "Content-Type:text/xml" -X PUT http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/DESCRIPTIVE -d '<dc:dublin-core xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>foo</dc:title></dc:dublin-core>'
$ curl -H "Content-Type:text/xml" -X PUT http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/SOURCE -d '<dc:dublin-core xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>foo</dc:title></dc:dublin-core>'
$ curl -H "Content-Type:text/xml" -X PUT http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/file-1/TECHNICAL -d '<textmd:textMD xmlns:textmd="info:lc/xmlns/textmd-v3"><textmd:encoding><textmd:encoding_platform linebreak="LF"></textmd:encoding_platform></textmd:encoding></textmd:textMD>'
$ curl -H "Content-Type:text/xml" -X PUT http://localhost:8080/fcrepo/rest/scape/metadata/entity-1/representation-1/file-1/bitstream-1/TECHNICAL -d '<dc:dublin-core xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>foo</dc:title></dc:dublin-core>'
```

#### Search Intellectual Entities:

```bash
$ curl -X GET "http://localhost:8080/fcrepo/rest/scape/sru/entities?version=1&operation=searchRetrieve&query=*"
```

#### Search Representations:

```bash
$ curl -X GET "http://localhost:8080/fcrepo/rest/scape/sru/representations?version=1&operation=searchRetrieve&query=*"
``` 

#### Search Files:

```bash
$ curl -X GET "http://localhost:8080/fcrepo/rest/scape/sru/files?version=1&operation=searchRetrieve&query=*"
```


Creating a WAR file from sources
--------------------------------

_If you're not sure you want to do this by yourself you can download a prepackaged WAR file from_ 
[SCAPE Fcrepo 4 War](https://drive.google.com/file/d/0B5nd_qlYdcqyM0pNbmJrSzh1dW8/edit?usp=sharing)

Since Fedora 4 is in active development and therefore the APIs used are not yet finalized, this project might not run on an arbitrary version of Fedora 4.



#### 1. Get Fedora 4

Checkout and build the tagged version of Fedora 4 from Github at https://github.com/futures/fcrepo4
OR download a prepackaged WAR from https://wiki.duraspace.org/display/FF/Downloads

```bash
$ git clone https://github.com/futures/fcrepo4.git
$ cd fcrepo4
$ git checkout fcrepo-4.0.0-scape
$ mvn clean install
```

#### 2. Deploy Fedora 4

Deploy the web application on a servlet container e.g. Apache Tomcat by copying the war file to the servlet container's webapp directory and start Fedora 4 so that the WAR file gets exploded.

```bash
$ cp fcrepo4/fcrepo-webapp/fcrepo-webapp-{VERSION}.war {TOMCAT_HOME}/webapps/fcrepo.war
$ {TOMCAT_HOME}/bin/catalina.sh run
```

#### 3. Create the Datamodel JAR

Checkout and build/install the Scape platform data model from  https://github.com/openplanets/scape-platform-datamodel

```bash
$ git clone https://github.com/openplanets/scape-platform-datamodel.git
$ cd scape-platform-datamodel
$ mvn clean install
```

#### 4. Create the API implementation JAR

Checkout and build/package the Connector API implementation from https://github.com/fasseg/fcrepo4-scape

```bash
$ git clone https://github.com/openplanets/scape-fcrepo4-connector.git
$ cd scape-fcrepo4-connector
$ mvn clean compile package
```	

#### 5. Install the JAR files

Copy the required JAR files from the data model and the Connector API to the Fedora 4 Webapp

```bash
$ cp scape-fcrepo4-connector/target/scape-fcrepo4-connector-{VERSION}.jar {TOMCAT_HOME}/webapps/fcrepo/WEB-INF/lib/
$ cp scape-platform-datamodel/target/scape-platform-datamodel-{VERSION}.jar {TOMCAT_HOME}/webapps/fcrepo/WEB-INF/lib/
```
	
#### 6. Update the web.xml

Update the configuration of the web application in order to have Fedora 4 discover the new HTTP endpoints at /scape/plans

*  Add "classpath:scape.xml" to the contextConfigLocation:

```xml
<context-param>
	<param-name>contextConfigLocation</param-name>
	<param-value>WEB-INF/classes/spring/*.xml classpath:scape.xml</param-value>
</context-param>
```

*  Add "eu.scape_project.resource" to the init parameter in order for Jersey to discover the new endpoint

```xml
<init-param>
	<param-name>com.sun.jersey.config.property.packages</param-name>
	<param-value>org.fcrepo, eu.scape_project.resource</param-value>
</init-param>
```
#### 7. Start the servlet container


```bash
$ {TOMCAT_HOME}/bin/catalina.sh run
```



Notes:
mvn install:install-file -Dfile=/home/ruckus/Downloads/pairtree-1.1.1.jar -DartifactId=pairtree -Dversion=1.1.1 -DgroupId=gov.loc -Dpackaging=jar
http://sourceforge.net/projects/loc-xferutils/files/loc-pairtree-java-library/pairtree-1.1.1.jar/download


