/*
 * Copyright 2021 Igor Nuzhnov
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import com.nuzigor.h3.H3
import com.uber.h3core.H3Core
import com.uber.h3core.exceptions.LineUndefinedException
import com.uber.h3core.util.GeoCoord
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{ParseException, WKTReader}

import scala.collection.JavaConverters._

/**
 * Return h3 addresses from a WKT geometry.
 *
 * @param wktExpr geometry in WKT format, supports POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON.
 * @param resolutionExpr h3 resolution
 */
@ExpressionDescription(
  usage = "_FUNC_(wkt, resolution) - Returns h3 addresses from a WKT geometry.",
  arguments = """
       Arguments:
         * wkt - geometry object in WKT format, supports POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON
             'POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))'
         * resolution - h3 index resolution
             7
     """,
  examples = """
       Examples:
         > SELECT _FUNC_('POLYGON ((3 -1, 3 -1.1, 3.1 1.1, 3 -1))', 7);
          [610279164519383039,610051464630370303,...]
         > SELECT _FUNC_('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))', 5);
          [601279802791428095,600536993566294015,601279518249844735,601045080681218047,...]
         > SELECT _FUNC_('POINT (-0.2983396 35.8466667)', 7);
          [599967133010493439]
         > SELECT _FUNC_('MULTIPOINT ((-0.2983396 35.8466667), (35.8466667 -0.2983396))', 7);
          [608974331292155903,610139512416239615]
         > SELECT _FUNC_('LINESTRING (-0.29 35.75, -0.30 35.83, -0.31 35.96)', 7);
          [608974331644477439,608974331711586303,608974331678031871,608974331241824255,...]
         > SELECT _FUNC_('MULTILINESTRING ((-0.29 35.75, -0.30 35.83, -0.31 35.96), (-1.29 38.75, -1.30 38.83, -1.31 38.96))', 7);
          [608974331644477439,608974331711586303,608974331678031871,608974331241824255,608974331208269823,...]
         > SELECT _FUNC_('MULTIPOLYGON (((3 -1, 3 -1.1, 3.1 1.1, 3 -1)), ((6 -1, 6 -1.1, 6.1 1.1, 6 -1)))', 7);
          [610279164519383039,610051464630370303,610051464546484223,610051508653785087,...]
         > SELECT _FUNC_('POINT EMPTY', 9);
          NULL
         > SELECT _FUNC_('POLYGON EMPTY', 10);
          NULL
     """,
  group = "array_funcs",
  since = "0.1.0")
case class ArrayFromWkt(wktExpr: Expression, resolutionExpr: Expression,
                        failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes with NullIntolerant {

  def this(wktExpr: Expression, resolutionExpr: Expression) =
    this(wktExpr, resolutionExpr, SQLConf.get.ansiEnabled)

  override def left: Expression = wktExpr
  override def right: Expression = resolutionExpr
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)
  override def dataType: DataType = ArrayType(LongType, containsNull = false)
  override def nullable: Boolean = true

  override protected def nullSafeEval(wktAny: Any, resolutionAny: Any): Any = {
    val wkt = wktAny.asInstanceOf[UTF8String].toString
    val resolution = resolutionAny.asInstanceOf[Int]
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      val h3Instance = H3.getInstance()
      val result = getGeometryIndices(h3Instance, geometry, resolution)
      if (result.isEmpty) {
        null
      } else {
        ArrayData.toArrayData(result)
      }
    } catch {
      case _: ParseException if !failOnError => null
      case _: LineUndefinedException if !failOnError => null
      case _: IllegalArgumentException if !failOnError => null
    }
  }

  private def getGeometryIndices(h3Instance: H3Core, geometry: Geometry, resolution: Int) = {
    geometry match {
      case geometry if geometry.isEmpty => Array.empty[Long]
      case polygon: Polygon => getPolygonIndices(h3Instance, polygon, resolution).distinct
      case point: Point => Array(h3Instance.geoToH3(point.getY, point.getX, resolution))
      case multiPoint: MultiPoint => gemMultiPointIndices(h3Instance, multiPoint, resolution)
      case multiPolygon: MultiPolygon => getMultiPolygonIndices(h3Instance, multiPolygon, resolution)
      case lineString: LineString => getLineStringIndices(h3Instance, lineString, resolution).distinct.toArray
      case multiLineString: MultiLineString => getMultiLineStringIndices(h3Instance, multiLineString, resolution)
      case _ => Array.empty[Long]
    }
  }

  private def getMultiLineStringIndices(h3Instance: H3Core, multiLineString: MultiLineString, resolution: Int) = {
    (0 until multiLineString.getNumGeometries)
      .map(multiLineString.getGeometryN(_).asInstanceOf[LineString])
      .flatMap(getLineStringIndices(h3Instance, _, resolution))
      .distinct
      .toArray
  }

  private def getMultiPolygonIndices(h3Instance: H3Core, multiPolygon: MultiPolygon, resolution: Int) = {
    (0 until multiPolygon.getNumGeometries)
      .map(multiPolygon.getGeometryN(_).asInstanceOf[Polygon])
      .flatMap(getPolygonIndices(h3Instance, _, resolution))
      .distinct
      .toArray
  }

  private def gemMultiPointIndices(h3Instance: H3Core, multiPoint: MultiPoint, resolution: Int) = {
    (0 until multiPoint.getNumGeometries)
      .map(multiPoint.getGeometryN(_).getCoordinate)
      .map(c => h3Instance.geoToH3(c.y, c.x, resolution))
      .distinct
      .toArray
  }

  private def getPolygonIndices(h3Instance: H3Core, polygon: Polygon, resolution: Int) = {
    val toGeoJavaList = (ring: LinearRing) => ring.getCoordinates.map(c => new GeoCoord(c.y, c.x)).toList.asJava
    val coordinates = toGeoJavaList(polygon.getExteriorRing)
    val holes = (0 until polygon.getNumInteriorRing).map(i => toGeoJavaList(polygon.getInteriorRingN(i))).toList.asJava
    h3Instance.polyfill(coordinates, holes, resolution).asScala.toArray
  }

  private def getLineStringIndices(h3Instance: H3Core, lineString: LineString, resolution: Int) = {
    val indices = lineString.getCoordinates.map(c => h3Instance.geoToH3(c.y, c.x, resolution))
    (0 until indices.length - 1)
      .flatMap(i => {
        val start = indices(i)
        val end = indices(i + 1)
        h3Instance
          .h3Line(start, end)
          .asScala
          .map(Long2long)
      })
  }
}
