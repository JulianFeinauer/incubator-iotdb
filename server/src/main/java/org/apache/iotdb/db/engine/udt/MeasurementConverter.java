/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.udt;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

/**
 * Converts a Measurement and its value from logical to physical
 */
@FunctionalInterface
public interface MeasurementConverter {

    Converted apply(String measurement, Object value);

    public static class Converted {

        private final List<String> measurements;
        private final List<Object> objects;
        private final List<TSDataType> dataTypes;

        public Converted(List<String> measurements, List<Object> objects, List<TSDataType> dataTypes) {
            this.measurements = measurements;
            this.objects = objects;
            this.dataTypes = dataTypes;
        }

        public List<String> getMeasurements() {
            return measurements;
        }

        public List<Object> getObjects() {
            return objects;
        }

        public List<TSDataType> getDataTypes() {
            return dataTypes;
        }
    }

}
