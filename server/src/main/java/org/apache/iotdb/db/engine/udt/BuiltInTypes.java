/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.udt;

import com.alibaba.fastjson.JSONValidator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Collections;

public class BuiltInTypes {

    public static final UserDefinedType JSON_TYPE = new UserDefinedType("JSON", true,
        TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY,
        o -> JSONValidator.from(o.toString()).validate(),
        (measurement, value) -> new MeasurementConverter.Converted(Collections.singletonList(measurement), Collections.singletonList(value.toString()), Collections.singletonList(TSDataType.TEXT)));

    public static final UserDefinedType EVEN_NUMBER = new UserDefinedType("EVEN_NUMBER", true,
        TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY,
        o -> Integer.parseInt(o.toString()) % 2 == 0,
        (measurement, value) -> new MeasurementConverter.Converted(Collections.singletonList(measurement), Collections.singletonList(value.toString()), Collections.singletonList(TSDataType.INT32)));

    private BuiltInTypes() {
        // Do not create
    }
}
