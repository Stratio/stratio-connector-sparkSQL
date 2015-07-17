package com.stratio.connector.sparksql.core.engine.query

import com.stratio.connector.commons.Loggable
import com.stratio.crossdata.common.logicalplan._
import com.stratio.crossdata.common.metadata.Operations

import scala.annotation.tailrec

/**
  * This class must process the query result from a logicalworkflow.
 */
case class PartialResultProcessor() extends  Loggable{

  import scala.collection.JavaConverters._


  val partialResultType = Set(Operations.SELECT_CROSS_JOIN_PARTIAL_RESULTS,Operations.PARTIAL_RESULTS,Operations.SELECT_FULL_OUTER_JOIN_PARTIAL_RESULTS,Operations.SELECT_INNER_JOIN_PARTIAL_RESULTS,Operations.SELECT_LEFT_OUTER_JOIN_PARTIAL_RESULTS,Operations.SELECT_RIGHT_OUTER_JOIN_PARTIAL_RESULTS)
  def findPartialResutltInProject(list: List[PartialResults],logicalStep: LogicalStep): List[PartialResults] = {
    def isPartialResult(p: Join): Boolean = {
      p.getOperations.asScala.subsetOf(partialResultType)
    }
    logicalStep match {
      case p: Join => {
        if (isPartialResult(p)) {
          logger.info(s"New partial result find with id ${p.getId}")
          list ::: findPartialResutltInProject(List(), p.getPreviousSteps.asScala.lastOption.orNull) ::: findPartialResutltInProject(List(), p.getNextStep)
        } else list
      }
      case pr: PartialResults =>
        list :+ pr
      case ls: LogicalStep =>
        findPartialResutltInProject(List(),ls.getNextStep)

      case _ =>
        List()
    }
  }


  def recoveredPartialResult(workflow: LogicalWorkflow) : Iterable[PartialResults] ={
   workflow.getInitialSteps.asScala.flatMap(ls => findPartialResutltInProject(List(),ls))
  }


}
