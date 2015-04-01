package com.stratio.connector.sparksql

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}

abstract class UnitTest(val component: String) extends {
  implicit val system = ActorSystem(s"${component}TestActorSystem")
} with FlatSpecLike
with TestKitBase
with Matchers
with BeforeAndAfterAll{

  behavior of component

  override def afterAll(): Unit ={
    super.afterAll()
    system.shutdown()
  }

}