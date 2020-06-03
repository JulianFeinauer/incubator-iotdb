/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.qp.strategy;

import com.alibaba.fastjson.JSON;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.InsertJsonOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.junit.Test;

import static org.junit.Assert.*;

public class PhysicalGeneratorTest {

    @Test
    public void test() throws QueryProcessException {
        PhysicalGenerator generator = new PhysicalGenerator();
        InsertJsonOperator operator = new InsertJsonOperator(0, "root.test.device", "{ \"a\": 1.0, \"b\": \"string\" }");

        PhysicalPlan physicalPlan = generator.transformToPhysicalPlan(operator);

        assertTrue(physicalPlan instanceof InsertPlan);
        InsertPlan insertPlan = (InsertPlan) physicalPlan;
        assertEquals("root.test.device", insertPlan.getDeviceId());
        assertArrayEquals(new String[]{"a", "b"}, insertPlan.getMeasurements());
        assertArrayEquals(new Object[]{"1.0", "string"}, insertPlan.getValues());
        assertTrue(insertPlan.isInferType());
    }
}