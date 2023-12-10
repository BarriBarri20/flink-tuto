/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playground.datagen.model;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/** Serializes a {@link HealthStatus} into a CSV record. */
public class HealthStatusSerializer implements Serializer<HealthStatus> {

  private static final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  @Override
  public void configure(Map<String, ?> map, boolean b) {}

  @Override
  public byte[] serialize(String s, HealthStatus transaction) {
    String csv =
        String.format(
            // Avoiding spaces here to workaround FLINK-23073
            "%s,%s,%s,%s,%s,%s",
            transaction.patientId, transaction.age, transaction.bmi, transaction.gender, transaction.timestamp.format(formatter));

    return csv.getBytes();
  }

  @Override
  public void close() {}
}
