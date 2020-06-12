/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;

public class CreateStructuredTimeSeriesPlan extends CreateTimeSeriesPlan {

    private Structure structure;

    public CreateStructuredTimeSeriesPlan(Path path, Structure structure, Map<String, String> props, Map<String, String> tags,
                                          Map<String, String> attributes, String alias) {
        super(false, Operator.OperatorType.CREATE_TIMESERIES);
        this.path = path;
        this.structure = structure;
        this.props = props;
        this.tags = tags;
        this.attributes = attributes;
        this.alias = alias;
    }

    public Structure getStructure() {
        return structure;
    }

    public void setStructure(Structure structure) {
        this.structure = structure;
    }

    public abstract static class Structure extends MeasurementSchema {

        public abstract Map<String, Structure> getElements();

        public abstract Structure getElement(String key);

    }

    public static class MapStructure extends Structure {

        private Map<String, Structure> elements;

        public MapStructure(Map<String, Structure> elements) {
            this.elements = elements;
        }

        @Override
        public Map<String, Structure> getElements() {
            return elements;
        }

        @Override
        public Structure getElement(String key) {
            return elements.get(key);
        }

    }

    public static class PrimitiveStructure extends Structure {

        private TSDataType dataType;
        private TSEncoding encoding;
        private CompressionType compressor;

        public PrimitiveStructure(TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
            this.dataType = dataType;
            this.encoding = encoding;
            this.compressor = compressor;
        }

        @Override
        public Map<String, Structure> getElements() {
            return Collections.emptyMap();
        }

        @Override
        public Structure getElement(String key) {
            throw new NoSuchElementException();
        }

        public TSDataType getDataType() {
            return dataType;
        }

        public void setDataType(TSDataType dataType) {
            this.dataType = dataType;
        }

        public TSEncoding getEncoding() {
            return encoding;
        }

        public void setEncoding(TSEncoding encoding) {
            this.encoding = encoding;
        }

        public CompressionType getCompressor() {
            return compressor;
        }

        public void setCompressor(CompressionType compressor) {
            this.compressor = compressor;
        }

    }

    @Override
    public String toString() {
        return "CreateStructuredTimeSeriesPlan{" +
            "structure=" + structure +
            ", path=" + path +
            ", alias='" + alias + '\'' +
            ", props=" + props +
            ", tags=" + tags +
            ", attributes=" + attributes +
            '}';
    }
}
