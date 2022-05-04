# H3 / Apache Spark SQL

Brings [H3 - Hexagonal hierarchical geospatial indexing system](https://h3geo.org/) support to [Apache Spark SQL](https://spark.apache.org/)

![Scala CI](https://github.com/nuzigor/h3-spark/actions/workflows/scala.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Installation
------------

Fetch the JAR file from Maven.

    libraryDependencies += "io.github.nuzigor" %% "h3-spark" % "0.8.0"

h3-spark supports only Spark 3.2.1+.

Function registration
--------------

Register h3 functions with these commands:

    com.nuzigor.spark.sql.h3.registerFunctions

Spark SQL extensions registration
--------------

Config your spark applications with `spark.sql.extensions` option: `spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions`

For example, to use in pure Spark SQL environment:

    spark-sql --packages io.github.nuzigor:h3-spark_2.12:0.8.0 --conf spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions

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
- h3_are_neighbors
- h3_cell_area_km2
- h3_cell_area_m2
- h3_cell_area_rads2
- h3_distance_km
- h3_distance_m
- h3_distance_rads
- h3_is_pentagon
- h3_k_ring_distances
