# H3 / Apache Spark SQL

Brings [H3 - Hexagonal hierarchical geospatial indexing system](https://h3geo.org/) support to [Apache Spark SQL](https://spark.apache.org/)

Installation
------------

Build with 'sbt assembly'

Add dependency to h3-spark.jar 

h3-spark supports only Spark 3+.

Function registration
--------------

Register h3 functions with these commands:

    com.nuzigor.spark.sql.h3.registerFunctions

Spark SQL extensions registration
--------------

Config your spark applications with `spark.sql.extensions` option: `spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions`

For example, to use in pure Spark SQL environment:

    spark-sql --jars Path/To/h3-spark.jar --conf spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions

Supported functions
--------------

- h3_from_geo
- h3_from_wkt
- h3_array_from_wkt
- h3_k_ring
- h3_hex_ring
- h3_line
- h3_distance
- h3_get_resolution
- h3_is_valid
- h3_to_parent
- h3_to_children
- h3_to_center_child
- h3_compact
- h3_uncompact