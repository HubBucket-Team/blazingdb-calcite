# WebView (BlazingDB workbench)

- This artifact is the bridge between the connectors and the engine (Simplicity)
- This artifact has some JSP views to manage and use the engine (Simplicity)

## Usage without Tomcat

```shell-script
export BLAZINGDB_DRIVER_SETTINGS=/path/to/driver.properties
java -jar blazing-workbench.jar
```

## driver.properties samples
### For MySQL

```shell-script
blazing.host=localhost
blazing.port=8890
jdbc.driverClassName=com.mysql.jdbc.Driver
jdbc.url=jdbc:mysql://localhost:3306/blazing
jdbc.username=blazing
jdbc.password=blazing
```

### For H2

```shell-script
blazing.host=localhost
blazing.port=8890
jdbc.driverClassName=org.h2.Driver
jdbc.url=jdbc:h2:/opt/blazing/workbench/data
jdbc.username=blazing
jdbc.password=blazing
```

### Maven Targets
```shell-script
mvn clean install #builds the target jar
mvn javadoc:jar #generates java docs in target/apidocs
```
