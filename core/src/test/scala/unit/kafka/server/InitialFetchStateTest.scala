/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.Uuid
import org.apache.kafka.server.network.BrokerEndPoint
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class InitialFetchStateTest {
  private val topicId = Some(Uuid.randomUuid())
  private val leader = new BrokerEndPoint(1, "localhost", 9092)

  @Test
  def testInitialFetchStateWithZeroLeaderEpoch(): Unit = {
    val initialFetchState = InitialFetchState(
      topicId = topicId,
      leader = leader,
      currentLeaderEpoch = 0,
      initOffset = 0L,
      readOnly = false
    )

    assertEquals(0, initialFetchState.currentLeaderEpoch)
  }

  @Test
  def testInitialFetchStateWithPositiveLeaderEpoch(): Unit = {
    val initialFetchState = InitialFetchState(
      topicId = topicId,
      leader = leader,
      currentLeaderEpoch = 5,
      initOffset = 1000L,
      readOnly = false
    )

    assertEquals(5, initialFetchState.currentLeaderEpoch)
  }

  @Test
  def testInitialFetchStateRequiresNonNegativeLeaderEpoch(): Unit = {
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      InitialFetchState(
        topicId = topicId,
        leader = leader,
        currentLeaderEpoch = -1,
        initOffset = 100L,
        readOnly = false
      )
    })

    assertTrue(exception.getMessage.contains("currentLeaderEpoch must be >= 0"))
    assertTrue(exception.getMessage.contains("was -1"))
  }

  @Test
  def testInitialFetchStateRequiresNonNegativeLeaderEpochWithLargeNegativeValue(): Unit = {
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      InitialFetchState(
        topicId = topicId,
        leader = leader,
        currentLeaderEpoch = -100,
        initOffset = 100L,
        readOnly = false
      )
    })

    assertTrue(exception.getMessage.contains("currentLeaderEpoch must be >= 0"))
    assertTrue(exception.getMessage.contains("was -100"))
  }
}
