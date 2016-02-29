/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package samza.examples.sql;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;

import java.util.Map;

/**
 * This task is very simple. Copy message from topic:word-raw and send it to topic:word-raw-output
 */
public class TopicReplicatorStreamTask implements StreamTask {
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "word-raw-output");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    byte[] payload = (byte[]) envelope.getMessage();
    System.out.print("hello word");
    System.out.println(envelope.getMessage().toString());
    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, payload));
  }
}
