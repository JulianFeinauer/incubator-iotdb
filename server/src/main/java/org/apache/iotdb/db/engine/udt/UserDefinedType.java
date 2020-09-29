/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.udt;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public class UserDefinedType {

    private final String name;
    private final boolean simple;
    private final TSDataType physicalType;
    private final TSEncoding encoding;
    private final CompressionType compressor;
    private final Predicate<Object> validator;
    private final Function<CreateTimeSeriesPlan, List<CreateTimeSeriesPlan>> planTransformer;
    private final MeasurementConverter transformer;

    public UserDefinedType(String name, boolean simple, TSDataType physicalType, TSEncoding encoding,
                           CompressionType compressor, Predicate<Object> validator,
                           Function<CreateTimeSeriesPlan, List<CreateTimeSeriesPlan>> planTransformer,
                           MeasurementConverter transformer) {
        this.name = name;
        this.simple = simple;
        this.physicalType = physicalType;
        this.encoding = encoding;
        this.compressor = compressor;
        this.validator = validator;
        this.planTransformer = planTransformer;
        this.transformer = transformer;
    }

    public boolean isSimple() {
        return simple;
    }

    public TSDataType getPhysicalType() {
        return physicalType;
    }

    public String getName() {
        return name;
    }

    public boolean validate(Object value) {
        return validator.test(value);
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public CompressionType getCompressor() {
        return compressor;
    }

    public MeasurementConverter.Converted transformToPhysical(String measurement, Object value) {
        return transformer.apply(measurement, value);
    }

    public List<CreateTimeSeriesPlan> transformCreatePlan(CreateTimeSeriesPlan plan) {
        return planTransformer.apply(plan);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserDefinedType that = (UserDefinedType) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
