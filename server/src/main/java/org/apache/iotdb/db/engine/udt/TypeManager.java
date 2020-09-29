/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.engine.udt;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeManager {

    private static final Logger logger = LoggerFactory.getLogger(TypeManager.class);

    private final Map<String, UserDefinedType> typeRepository = new HashMap<>();

    {
        typeRepository.put("JSONS", BuiltInTypes.JSONS);
        typeRepository.put("EVEN_NUMBER", BuiltInTypes.EVEN_NUMBER);
        typeRepository.put("GPS", BuiltInTypes.GPS);
    }

    private final Map<PartialPath, UserDefinedType> assignedTypes = new HashMap<>();

    private static class TypeManagerHolder {

        private TypeManagerHolder() {
            // allowed to do nothing
        }

        private static final TypeManager INSTANCE = new TypeManager();


    }
    /**
     * we should not use this function in other place, but only in IoTDB class
     */
    public static TypeManager getInstance() {
        return TypeManager.TypeManagerHolder.INSTANCE;
    }
    private TypeManager() {
    }

    public List<CreateTimeSeriesPlan> makePhysicalPlans(CreateTimeSeriesPlan plan) {
        System.out.println("Checking Create PLAN " + plan.toString());

        if (plan.getLogicalType() != null) {
            if (plan.getDataType() != TSDataType.UDT) {
                throw new TypeManagerException("Logical Type is given but Data Type is not UDT but " + plan.getDataType());
            }
            // Validate that logical type exists
            if (!typeRepository.containsKey(plan.getLogicalType())) {
                throw new TypeManagerException("Unknown User Defined Type with ID " + plan.getLogicalType());
            }
            // Store it, and map it to the respective physical type
            final UserDefinedType type = typeRepository.get(plan.getLogicalType());
            List<CreateTimeSeriesPlan> plans = type.transformCreatePlan(plan);
            assignedTypes.put(plan.getPath(), type);
            return plans;

//            if (type.isSimple()) {
//                final TSDataType physicalType = type.getPhysicalType();
//                // Store that we mapped the path to this type
//                System.out.println("Store path " + plan.getPath().toString() + " as type " + type.getName());
//                return new CreateTimeSeriesPlan(plan.getPath(), physicalType, type.getEncoding(), type.getCompressor(), plan.getProps(), plan.getTags(), plan.getAttributes(), plan.getAlias());
//            } else {
//                throw new NotImplementedException("Only simple types are implemented now!");
//            }
        } else {
            return Collections.singletonList(plan);
        }

    }

    public InsertRowPlan makePhysicalPlans(InsertRowPlan logicalPlan) {
        System.out.println("Checking Insert PLAN " + logicalPlan.toString());

        PartialPath appendix;
        try {
            appendix = new PartialPath("");
        } catch (IllegalPathException e) {
            throw new RuntimeException("This should never happen", e);
        }
        List<String> measurements = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<Object> insertValues = new ArrayList<>();

        for (int i = 0; i < logicalPlan.getMeasurements().length; i++) {
            final String measurement = logicalPlan.getMeasurements()[i];
            final TSDataType dataType = logicalPlan.getDataTypes()[i];

            System.out.println("Checking measurement " + measurement + " with type " + dataType);

            try {
                final PartialPath path = logicalPlan.getDeviceId().concatPath(new PartialPath(measurement));

                System.out.println("Checking path " + path.toString());

                if (assignedTypes.containsKey(path)) {
                    //
                    System.out.println("We have a complex type here: " + path.toString());
                    final UserDefinedType type = assignedTypes.get(path);

                    final Object value = logicalPlan.getValues()[i];
                    final boolean isValid = type.validate(value);

                    if (!isValid) {
                        throw new TypeManagerException("The value '" + value.toString() + "' is not applicable to type " + type.getName());
                    }

                    // Now transform the Plan accordingly
                    final MeasurementConverter.Converted transformed = type.transformToPhysical(measurement, value);

                    // Add them to the lists
                    appendix = transformed.getPathAppendix();
                    measurements.addAll(transformed.getMeasurements());
                    insertValues.addAll(transformed.getObjects());
                    dataTypes.addAll(transformed.getDataTypes());
                } else {
                    // Simply take the values we already had
                    measurements.add(measurement);
                    insertValues.add(logicalPlan.getValues()[i].toString());
                    dataTypes.add(null);
                }
            } catch (IllegalPathException e) {
                throw new TypeManagerException("Unable to infer full path", e);
            }
        }

        final InsertRowPlan insertRowPlan;
        insertRowPlan = new InsertRowPlan(logicalPlan.getDeviceId().concatPath(appendix), logicalPlan.getTime(), measurements.toArray(new String[0]), measurements.toArray(new String[0]));
        insertRowPlan.setValues(insertValues.toArray(new Object[0]));
        insertRowPlan.setDataTypes(dataTypes.toArray(new TSDataType[0]));
        // insertRowPlan.setNeedInferType(false);
        return insertRowPlan;
    }
}
