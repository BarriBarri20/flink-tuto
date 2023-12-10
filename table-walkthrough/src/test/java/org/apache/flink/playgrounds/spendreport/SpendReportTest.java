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

package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A unit test of the spend report.
 * If this test passes then the business
 * logic is correct.
 */
public class SpendReportTest {

    private static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0);
    
    @Test
    public void testReport() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table transactions =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("patient_id", DataTypes.BIGINT()),
                                DataTypes.FIELD("isSmoker", DataTypes.BIGINT()),
                                DataTypes.FIELD("age", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("gender", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("bmi", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("transaction_time", DataTypes.TIMESTAMP(3))),

        Row.of(1, 188, 32, 32, 324, DATE_TIME.plusMinutes(12)),
                        Row.of(6, 188, 32, 32, 324, DATE_TIME.plusMinutes(32)),
                        Row.of(2, 188, 32, 32, 324, DATE_TIME.plusMinutes(42)),
                        Row.of(3, 188, 32, 32, 324, DATE_TIME.plusMinutes(52)),
                        Row.of(4, 188, 32, 32, 324, DATE_TIME.plusMinutes(62)),
                        Row.of(5, 188, 32, 32, 324, DATE_TIME.plusMinutes(112)));


        try {
            TableResult results = SpendReport.report(transactions).execute();

        } catch (UnimplementedException e) {
            Assume.assumeNoException("The walkthrough has not been implemented", e);
        }
    }
    
    private static List<Row> materialize(TableResult results) {
        try (CloseableIterator<Row> resultIterator = results.collect()) {
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize results", e);
        }
    }
}
