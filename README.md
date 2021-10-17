# H3 spark

Brings [Uber H3](https://h3geo.org/) support to Apache Spark SQL

Installation
------------

Add dependency to h3-spark.jar 

h3-spark supports only Spark 3+.

Function registration
--------------

Register h3 functions with these commands:::

    com.nuzigor.spark.sql.h3.registerFunctions

Spark SQL extensions registration
--------------

Config your spark applications with `spark.sql.extensions`, e.g. `spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions`
