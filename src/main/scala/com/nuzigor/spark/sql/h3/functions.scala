/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

object functions {
  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
   * Computes h3 address from latitude and longitude at target resolution.
   * @param latitude latitude in degrees
   * @param longitude longitude in degrees
   * @param resolution target resolution
   * @return h3 address
   */
  def h3_from_latlng(latitude: Column, longitude: Column, resolution: Int): Column = withExpr {
    FromLatLng(latitude.expr, longitude.expr, Literal(resolution))
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
  def h3_grid_disk(origin: Column, k: Int): Column = withExpr {
    GridDisk(origin.expr, Literal(k))
  }

  /**
   * Computes h3 indices within k distance of the origin index.
   * @param origin h3 index
   * @param k distance
   * @return array of arrays of h3 indices
   */
  def h3_grid_disk_distances(origin: Column, k: Int): Column = withExpr {
    GridDiskDistances(origin.expr, Literal(k))
  }

  /**
   * Computes hollow hexagonal ring centered at origin with sides of length k.
   * @param origin h3 index
   * @param k distance
   * @return array of h3 indices
   */
  def h3_grid_ring(origin: Column, k: Int): Column = withExpr {
    GridRing(origin.expr, Literal(k))
  }

  /**
   * Computes the line of indices between start and end.
   * @param start start h3 index
   * @param end end h3 index
   * @return array of h3 indices
   */
  def h3_grid_path(start: Column, end: Column): Column = withExpr {
    GridPath(start.expr, end.expr)
  }

  /**
   * Computes the distance in grid cells between start and end.
   * @param start start h3 index
   * @param end end h3 index
   * @return distance between cells
   */
  def h3_grid_distance(start: Column, end: Column): Column = withExpr {
    GridDistance(start.expr, end.expr)
  }

  /**
   * Gives the "great circle" or "haversine" distance between centers of h3 indices in radians.
   * @param start start h3 index
   * @param end end h3 index
   * @return distance between centers of cells in radians
   */
  def h3_distance_rads(start: Column, end: Column): Column = withExpr {
    GreatCircleDistanceRads(start.expr, end.expr)
  }

  /**
   * Gives the "great circle" or "haversine" distance between centers of h3 indices in meters.
   * @param start start h3 index
   * @param end end h3 index
   * @return distance between centers of cells in meters
   */
  def h3_distance_m(start: Column, end: Column): Column = withExpr {
    GreatCircleDistanceM(start.expr, end.expr)
  }

  /**
   * Gives the "great circle" or "haversine" distance between centers of h3 indices in kilometers.
   * @param start start h3 index
   * @param end end h3 index
   * @return distance between centers of cells in kilometers
   */
  def h3_distance_km(start: Column, end: Column): Column = withExpr {
    GreatCircleDistanceKm(start.expr, end.expr)
  }

  /**
   * Gives the exact area of specific cell in square radians.
   * @param h3 h3 index
   * @return cell area in square radians
   */
  def h3_cell_area_rads2(h3: Column): Column = withExpr {
    CellAreaRads2(h3.expr)
  }

  /**
   * Gives the exact area of specific cell in square meters.
   * @param h3 h3 index
   * @return cell area in square meters
   */
  def h3_cell_area_m2(h3: Column): Column = withExpr {
    CellAreaM2(h3.expr)
  }

  /**
   * Gives the exact area of specific cell in square kilometers.
   * @param h3 h3 index
   * @return cell area in square kilometers
   */
  def h3_cell_area_km2(h3: Column): Column = withExpr {
    CellAreaKm2(h3.expr)
  }

  /**
   * Returns whether or not the provided h3 indexes are neighbors.
   * @param origin origin h3 index
   * @param destination destination h3 index
   * @return true if the provided h3 indices are neighbors
   */
  def h3_are_neighbors(origin: Column, destination: Column): Column = withExpr {
    AreNeighbors(origin.expr, destination.expr)
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
   * Returns the base cell number for h3 index
   *
   * @param h3 h3 index
   * @return base cell number of h3 index
   */
  def h3_base_cell_number(h3: Column): Column = withExpr {
    GetBaseCellNumber(h3.expr)
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
   * Returns true if h3 index represents a pentagon cell
   * @param h3 h3 index
   * @return true if pentagon cell, false otherwise
   */
  def h3_is_pentagon(h3: Column): Column = withExpr {
    IsPentagon(h3.expr)
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
}
