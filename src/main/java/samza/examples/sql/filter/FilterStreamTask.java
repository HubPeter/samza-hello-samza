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

package samza.examples.sql.filter;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.util.Arrays;

/**
 * This task is very simple. Copy message from topic:word-raw and send it to topic:word-raw-output
 */
public class FilterStreamTask implements WindowableTask, InitableTask, StreamTask {
    private static SystemStream OUTPUT_STREAM;
    private static final SystemStream COUNTER_OUTPUT_STREAM = new SystemStream("kafka", "samza-filter-counter-output");

    private static int counterProcessed = 0;
    private static int counterMatched = 0;
    private static Long lastClock = System.currentTimeMillis();
    private static final int COUNTER_BATCH = 100000;

    private final String fieldSplit = ",";
    private int filterColId;

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {

//        if(envelope!=null)
        counterProcessed++;
//        String msg = (String) envelope.getMessage();
////        System.out.println(msg);
////        System.out.println(filterColId);
//        String[] values = msg.split(fieldSplit, filterColId);
////        System.out.println(Arrays.toString(values));
//        // filter
//        if (values != null && values.length > filterColId) {
//            if (values[filterColId] != null &&
//                    (values[filterColId].equals("Word1")
//                            || values[filterColId].equals("Word2")
//                            || values[filterColId].equals("Word3")
//                    )) {
////                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, msg));
//                counterMatched++;
//            }
//        }
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        filterColId = Integer.parseInt(config.get("systems.kafka.filter.colid"));
        // output stream
        String outputStreamName = config.get("systems.kafka.filter.outputstream");
        OUTPUT_STREAM = new SystemStream("kafka", outputStreamName.substring(5));
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        collector.send(new OutgoingMessageEnvelope(COUNTER_OUTPUT_STREAM,
                "filter process:" + counterProcessed
                        + "    match:" + counterMatched + "   "
                        + counterProcessed * 1000 / (System.currentTimeMillis() - lastClock)
                        + " msg/s"));
        lastClock = System.currentTimeMillis();
        counterProcessed = 0;
        counterMatched = 0;
    }
}
