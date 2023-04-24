/*
 * Copyright 2021 Igor Nuzhnov
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.nuzigor.spark.sql.h3

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ExpressionInfo}

import scala.reflect.ClassTag

object FunctionCatalog {
  lazy val functions: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = Seq(
    createFunctionDescription[FromLatLng]("h3_from_latlng"),
    createFunctionDescription[FromWkt]("h3_from_wkt"),
    createFunctionDescription[ArrayFromWkt]("h3_array_from_wkt"),
    createFunctionDescription[GridDisk]("h3_grid_disk"),
    createFunctionDescription[GridRing]("h3_grid_ring"),
    createFunctionDescription[GridPath]("h3_grid_path"),
    createFunctionDescription[GridDistance]("h3_grid_distance"),
    createFunctionDescription[GetResolution]("h3_get_resolution"),
    createFunctionDescription[IsValid]("h3_is_valid"),
    createFunctionDescription[ToParent]("h3_to_parent"),
    createFunctionDescription[ToChildren]("h3_to_children"),
    createFunctionDescription[ToCenterChild]("h3_to_center_child"),
    createFunctionDescription[Compact]("h3_compact"),
    createFunctionDescription[Uncompact]("h3_uncompact"),
    createFunctionDescription[AreNeighbors]("h3_are_neighbors"),
    createFunctionDescription[CellAreaKm2]("h3_cell_area_km2"),
    createFunctionDescription[CellAreaM2]("h3_cell_area_m2"),
    createFunctionDescription[CellAreaRads2]("h3_cell_area_rads2"),
    createFunctionDescription[GreatCircleDistanceKm]("h3_distance_km"),
    createFunctionDescription[GreatCircleDistanceM]("h3_distance_m"),
    createFunctionDescription[GreatCircleDistanceRads]("h3_distance_rads"),
    createFunctionDescription[IsPentagon]("h3_is_pentagon"),
    createFunctionDescription[GridDiskDistances]("h3_grid_disk_distances")
  )

  private def createFunctionDescription[T <: Expression](name: String)(implicit tag: ClassTag[T]): (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val constructors = tag.runtimeClass.getConstructors
    val builder = (expressions: Seq[Expression]) => {
      val params = Seq.fill(expressions.size)(classOf[Expression])
      val f = constructors.find(_.getParameterTypes.toSeq equals params).getOrElse {
        val validParametersCount = constructors
          .filter(_.getParameterTypes.forall(_ equals classOf[Expression]))
          .map(_.getParameterCount)
          .distinct
          .sorted
        val invalidArgumentsMsg = validParametersCount.length match {
          case 0 => s"Invalid arguments for function $name"
          case _ =>
            val expectedNumberOfParameters = validParametersCount.length match {
              case 1 => validParametersCount.head.toString
              case _ =>
                validParametersCount.init.mkString("one of ", ", ", " and ") +
                  validParametersCount.last
            }
            s"Invalid number of arguments for function $name. " +
              s"Expected: $expectedNumberOfParameters; Found: ${params.length}"
        }
        throw new Exception(invalidArgumentsMsg)
      }

      try {
        f.newInstance(expressions: _*).asInstanceOf[Expression]
      } catch {
        case e: Exception => throw new Exception(e.getCause.getMessage)
      }
    }

    val expressionInfo = createExpressionInfo[T](name)
    (FunctionIdentifier(name), expressionInfo, builder)
  }

  private def createExpressionInfo[T <: Expression: ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    Option(clazz.getAnnotation(classOf[ExpressionDescription])) match {
      case Some(ed) =>
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          ed.usage(),
          ed.arguments(),
          ed.examples(),
          ed.note(),
          ed.group(),
          ed.since(),
          ed.deprecated(),
          ed.source()
        )
      case None => new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }
}
