/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

// scalastyle:off
object functions {
// scalastyle:on
  private def withExpr(expr: Expression): Column = new Column(expr)

  // scalastyle:off method.name
  /**
   * Computes h3 address from latitude and longitude at target resolution.
   * @param latitude latitude in degrees
   * @param longitude longitude in degrees
   * @param resolution target resolution
   * @return h3 address
   */
  def h3_from_geo(latitude: Column, longitude: Column, resolution: Int): Column = withExpr {
    FromGeo(latitude.expr, longitude.expr, Literal(resolution))
  }

  /**
   * Computes h3 address from a WKT string at target resolution.
   * @param wkt point object in WKT format
   * @param resolution target resolution
   * @return h3 address
   */
  def h3_from_wkt(wkt: Column, resolution: Int): Column = withExpr {
    FromWkt(wkt.expr, Literal(resolution))
  }

  /**
   * Computes h3 addresses from a WKT geometry.
   * @param wkt geometry object in WKT format, supports POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON
   * @param resolution target resolution
   * @return array of h3 addresses
   */
  def h3_array_from_wkt(wkt: Column, resolution: Int): Column = withExpr {
    ArrayFromWkt(wkt.expr, Literal(resolution))
  }

  /**
   * Computes h3 indices within k distance of the origin index.
   * @param origin h3 index
   * @param k distance
   * @return array of h3 indices
   */
  def h3_k_ring(origin: Column, k: Int): Column = withExpr {
    KRing(origin.expr, Literal(k))
  }

  /**
   * Computes hollow hexagonal ring centered at origin with sides of length k.
   * @param origin h3 index
   * @param k distance
   * @return array of h3 indices
   */
  def h3_hex_ring(origin: Column, k: Int): Column = withExpr {
    HexRing(origin.expr, Literal(k))
  }

  /**
   * Computes the line of indices between start and end.
   * @param start start h3 index
   * @param end end h3 index
   * @return array of h3 indices
   */
  def h3_line(start: Column, end: Column): Column = withExpr {
    Line(start.expr, end.expr)
  }

  /**
   * Computes the distance in grid cells between start and end.
   * @param start start h3 index
   * @param end end h3 index
   * @return distance between cells
   */
  def h3_distance(start: Column, end: Column): Column = withExpr {
    Distance(start.expr, end.expr)
  }

  /**
   * Returns the resolution of h3 index.
   * @param h3 h3 index
   * @return resolution
   */
  def h3_get_resolution(h3: Column): Column = withExpr {
    GetResolution(h3.expr)
  }

  /**
   * Returns true if h3 index is valid
   * @param h3 h3 index
   * @return true if valid, false otherwise
   */
  def h3_is_valid(h3: Column): Column = withExpr {
    IsValid(h3.expr)
  }

  /**
   * Returns the parent (coarser) index containing h3.
   * @param h3 child h3 index
   * @param parentResolution parent index resolution
   * @return parent h3 index
   */
  def h3_to_parent(h3: Column, parentResolution: Int): Column = withExpr {
    ToParent(h3.expr, Literal(parentResolution))
  }

  /**
   * Returns h3 indices contained within of the original index and child resolution.
   * @param h3 parent h3 index
   * @param childResolution child resolution
   * @return array of child h3 indices
   */
  def h3_to_children(h3: Column, childResolution: Int): Column = withExpr {
    ToChildren(h3.expr, Literal(childResolution))
  }

  /**
   * Computes the center child of h3 index at child resolution.
   * @param h3 parent h3 index
   * @param childResolution child resolution
   * @return child h3 index
   */
  def h3_to_center_child(h3: Column, childResolution: Int): Column = withExpr {
    ToCenterChild(h3.expr, Literal(childResolution))
  }

  /**
   * Returns the compacted array on indices.
   * @param h3 array of h3 indices
   * @return compacted array oh h3 indices
   */
  def h3_compact(h3: Column): Column = withExpr {
    Compact(h3.expr)
  }

  /**
   * Un-compacts the set of indices using the target resolution.
   * @param h3 array of h3 indices
   * @param resolution target resolution
   * @return un-compacted array of h3 indices
   */
  def h3_uncompact(h3: Column, resolution: Int): Column = withExpr {
    Uncompact(h3.expr, Literal(resolution))
  }
  // scalastyle:on method.name
}
