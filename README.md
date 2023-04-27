# H3 / Apache Spark SQL

Brings [H3 - Hexagonal hierarchical geospatial indexing system](https://h3geo.org/) support to [Apache Spark SQL](https://spark.apache.org/)

![Scala CI](https://github.com/nuzigor/h3-spark/actions/workflows/scala.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.nuzigor/h3-spark_2.12?label=Maven%20Central&color=%236DBE42)](https://central.sonatype.com/search?q=g%253Aio.github.nuzigor%2520a%253Ah3-spark_2.12)

Installation
------------

Fetch the JAR file from Maven.

    libraryDependencies += "io.github.nuzigor" %% "h3-spark" % "0.9.1"

h3-spark supports only Spark 3.2.1+.

Function registration
--------------

Register h3 functions with these commands:

    com.nuzigor.spark.sql.h3.registerFunctions

Spark SQL extensions registration
--------------

Config your spark applications with `spark.sql.extensions` option: `spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions`

For example, to use in pure Spark SQL environment:

    spark-sql --packages io.github.nuzigor:h3-spark_2.12:0.9.1 --conf spark.sql.extensions=com.nuzigor.spark.sql.h3.H3SqlExtensions

Supported functions
--------------

| Spark function         | Corresponding h3 function                   |
|------------------------|---------------------------------------------|
| h3_from_latlng         | latLngToCell                                |
| h3_from_wkt            | latLngToCell                                |
| h3_array_from_wkt      | latLngToCell, polygonToCells, gridPathCells |
| h3_grid_disk           | gridDisk                                    |
| h3_grid_ring           | gridRingUnsafe                              |
| h3_grid_path           | gridPathCells                               |
| h3_grid_distance       | gridDistance                                |
| h3_get_resolution      | getResolution                               |
| h3_is_valid            | isValidCell                                 |
| h3_to_parent           | cellToParent                                |
| h3_to_children         | cellToChildren                              |
| h3_to_center_child     | cellToCenterChild                           |
| h3_compact             | compactCells                                |
| h3_uncompact           | uncompactCells                              |
| h3_are_neighbors       | areNeighborCells                            |
| h3_cell_area_km2       | cellArea                                    |
| h3_cell_area_m2        | cellArea                                    |
| h3_cell_area_rads2     | cellArea                                    |
| h3_distance_km         | greatCircleDistance                         |
| h3_distance_m          | greatCircleDistance                         |
| h3_distance_rads       | greatCircleDistance                         |
| h3_is_pentagon         | isPentagon                                  |
| h3_grid_disk_distances | gridDiskDistances                           |

Unsupported functions
--------------

- getBaseCellNumber
- stringToH3
- h3ToString
- isResClassIII
- getIcosahedronFaces
- cellToLatLng
- cellToBoundary
- gridDiskUnsafe
- gridDiskDistancesUnsafe
- gridDiskDistancesSafe
- gridDisksUnsafe
- cellToLocalIj
- localIjToCell
- cellToChildPos
- childPosToCell
- cellsToLinkedMultiPolygon
- cellsToMultiPolygon
- cellsToDirectedEdge
- isValidDirectedEdge
- getDirectedEdgeOrigin
- getDirectedEdgeDestination
- directedEdgeToCells
- originToDirectedEdges
- directedEdgeToBoundary
- cellToVertex
- cellToVertexes
- vertexToLatLng
- isValidVertex
- getHexagonAreaAvg
- getHexagonEdgeLengthAvg
- edgeLength
- getNumCells
- getRes0Cells
- getPentagons
