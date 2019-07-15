# BlazingDB Calcite

# How to run Calcite Application

To launch calcite-application with default arguments just run

```shell-script
java -jar BlazingCalcite.jar -p CALCITE_PROTOCOL_TCP_PORT --data_directory=DIR_PATH_WHERE_IS_THE_H2_CATALOG_DATABASE

#Example for localhost:
java -jar BlazingCalcite.jar -p 8890 --data_directory=/blazingsql
```
