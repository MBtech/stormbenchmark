/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.benchmark.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/*
 * Listens for all metrics, dumps them to log
 *
 * To use, add this to your topology's configuration:
 *   conf.registerMetricsConsumer(org.apache.storm.metrics.LoggingMetricsConsumer.class, 1);
 *
 * Or edit the storm.yaml config file:
 *
 *   topology.metrics.consumer.register:
 *     - class: "org.apache.storm.metrics.LoggingMetricsConsumer"
 *       parallelism.hint: 1
 *
 */
public class TempConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(TempConsumer.class);
    static transient DateFormat dateFormat;
    static transient Calendar cal;
    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) { 
    dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    }

    static private String padding = "                       ";

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
        cal = Calendar.getInstance();
        String header = String.format("%s%d%15s:%-4d%3d:%-11s",
            dateFormat.format(cal.getTime()),
            taskInfo.timestamp,
            taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
            taskInfo.srcTaskId,
            taskInfo.srcComponentId);
        sb.append(header);
        for (DataPoint p : dataPoints) {
            sb.delete(header.length(), sb.length());
            sb.append(p.name)
                .append(padding).delete(header.length()+23,sb.length()).append("\t")
                .append(p.value);
            LOG.info(sb.toString());
        }
    }

    @Override
    public void cleanup() { }
}
