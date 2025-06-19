/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.runtime.traversal.operator;

import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.TreePaths;
import com.antgroup.geaflow.dsl.runtime.util.SchemaUtil;
import com.antgroup.geaflow.dsl.runtime.util.StepFunctionUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StepFilterOperator extends AbstractStepOperator<StepBoolFunction, StepRecordWithPath, StepRecordWithPath> {

    private final int[] refPathIndices;
    private final boolean isOptional;

    public StepFilterOperator(long id, StepBoolFunction function) {
        super(id, function);
        this.refPathIndices = StepFunctionUtil.getRefPathIndices(function);
        this.isOptional = function.isOptional();
    }

    @Override
    public void processRecord(StepRecordWithPath record) {
        if (isOptional) {
            // Optional match semantics following StepJoinOperator LEFT JOIN pattern:
            // Compare original record with filtered record, and replace removed parts with null
            StepRecordWithPath filterRecord = record.filterOptional(path -> function.filter(withParameter(path)), refPathIndices);

            // Create optional result by merging original and filtered records
            // This preserves the structure but replaces filtered-out parts with null values
            collect(filterRecord,isOptional);
        } else {
            // Regular filtering behavior - only collect if filter passes
            StepRecordWithPath filterRecord = record.filter(path -> function.filter(withParameter(path)), refPathIndices);
            if (!filterRecord.isPathEmpty()) {
                collect(filterRecord);
            }
        }
    }


    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepFilterOperator(id, function);
    }
}
