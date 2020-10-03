/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.udt;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class BuiltInTypes {

    // No unrolling
    public static final UserDefinedType JSONS = new UserDefinedType("JSONS", true,
        TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY,
        o -> JSONValidator.from(o.toString()).validate(),
        plan -> {
            return Collections.singletonList(
                new CreateTimeSeriesPlan(plan.getPath(), TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY, plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias())
            );
        },
        (measurement, value) -> new MeasurementConverter.Converted(Collections.singletonList(measurement), Collections.singletonList(value.toString()), Collections.singletonList(TSDataType.TEXT)));

    // Unrolling
    public static final UserDefinedType JSON = new UserDefinedType("JSON", true,
        TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY,
        o -> JSONValidator.from(o.toString()).validate(),
        plan -> {
            return Collections.singletonList(
                new CreateTimeSeriesPlan(plan.getPath(), TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY, plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias())
            );
        },
        (measurement, value) -> new MeasurementConverter.Converted(Collections.singletonList(measurement), Collections.singletonList(value.toString()), Collections.singletonList(TSDataType.TEXT)));

    public static final UserDefinedType EVEN_NUMBER = new UserDefinedType("EVEN_NUMBER", true,
        TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY,
        o -> Integer.parseInt(o.toString()) % 2 == 0,
        plan -> {
            // Store that we mapped the path to this type
            return Collections.singletonList(
                new CreateTimeSeriesPlan(plan.getPath(), TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY, plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias())
            );
        },
        (measurement, value) -> new MeasurementConverter.Converted(Collections.singletonList(measurement), Collections.singletonList(value.toString()), Collections.singletonList(TSDataType.INT32)));

    public static final UserDefinedType GPS = new UserDefinedType("GPS", true,
        TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY,
        o -> {
            if (!JSONValidator.from(o.toString()).validate()) {
                return false;
            }
            final JSONObject jsonObject = (JSONObject) JSONObject.parse(o.toString());
            if (!(new HashSet<>(Arrays.asList("lat", "lon"))).equals(jsonObject.keySet())) {
                return false;
            }
            return jsonObject.get("lat") instanceof Number &&
                jsonObject.get("lon") instanceof Number;
        },
        plan -> {
            // Store that we mapped the path to this type
            return Arrays.asList(
                new CreateTimeSeriesPlan(concat(plan.getPath(), "lat"), TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias()),
                new CreateTimeSeriesPlan(concat(plan.getPath(), "lon"), TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY, plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias())
            );
        },
        (measurement, value) -> {
            final JSONObject jsonObject = (JSONObject) JSONObject.parse(value.toString());
            final PartialPath appendix;
            try {
                appendix = new PartialPath("gps");
            } catch (IllegalPathException e) {
                throw new IllegalStateException("This should never happen!", e);
            }
            return new MeasurementConverter.Converted(
                appendix,
                Arrays.asList("lat", "lon"),
                Arrays.asList(jsonObject.get("lat"), jsonObject.get("lon")),
                Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
        });


    private static PartialPath concat(PartialPath path, String appendix) {
        try {
            return path.concatPath(new PartialPath(appendix));
        } catch (IllegalPathException e) {
            throw new IllegalStateException("This should never happen!", e);
        }
    }

    private BuiltInTypes() {
        // Do not create
    }
}
