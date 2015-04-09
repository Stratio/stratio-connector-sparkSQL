/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.stratio.connector.sparksql

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKitBase}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}

abstract class Test(val component: String) extends {
  implicit val system = ActorSystem(s"${component}TestActorSystem")
} with FlatSpecLike
with TestKitBase
with ImplicitSender
with Matchers
with BeforeAndAfterAll{

  behavior of component

  override def afterAll(): Unit ={
    super.afterAll()
    system.shutdown()
  }

}