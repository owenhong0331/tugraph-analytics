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
            StepRecordWithPath optionalRecord = createOptionalRecord(record, filterRecord);
            collect(filterRecord);
        } else {
            // Regular filtering behavior - only collect if filter passes
            StepRecordWithPath filterRecord = record.filter(path -> function.filter(withParameter(path)), refPathIndices);
            if (!filterRecord.isPathEmpty()) {
                collect(filterRecord);
            }
        }
    }

    /**
     * Create optional record by comparing original and filtered records.
     * This follows StepJoinOperator LEFT JOIN pattern: preserve original structure
     * but replace filtered-out parts with null values.
     */
    private StepRecordWithPath createOptionalRecord(StepRecordWithPath originalRecord, StepRecordWithPath filterRecord) {
        // if (originalRecord.getType() == StepRecordType.VERTEX) {
        //     return createOptionalVertexRecord((VertexRecord) originalRecord, (VertexRecord) filterRecord);
        // } else if (originalRecord.getType() == StepRecordType.EDGE_GROUP) {
        //     return createOptionalEdgeGroupRecord((EdgeGroupRecord) originalRecord, (EdgeGroupRecord) filterRecord);
        // } else {
        //     // For other record types, return filtered record if not empty, otherwise original
        //     return filterRecord.isPathEmpty() ? originalRecord : filterRecord;
        // }
        return filterRecord.isPathEmpty() ? originalRecord : filterRecord;
    }

    /**
     * Create optional VertexRecord by merging original and filtered records.
     * Compare each path node and fill null for filtered-out positions.
     */
    private VertexRecord createOptionalVertexRecord(VertexRecord originalRecord, VertexRecord filterRecord) {
        ITreePath originalTreePath = originalRecord.getTreePath();
        ITreePath filteredTreePath = filterRecord.getTreePath();
        Object vertexId = originalRecord.getVertex().getId();

        // Always compare original and filtered paths node by node
        // Fill null for positions that were filtered out
        ITreePath mergedTreePath = mergeTreePaths(originalTreePath, filteredTreePath);

        return VertexRecord.of(IdOnlyVertex.of(vertexId), mergedTreePath);
    }

    /**
     * Merge original and filtered tree paths, filling null for filtered-out nodes.
     */
    private ITreePath mergeTreePaths(ITreePath originalTreePath, ITreePath filteredTreePath) {
        if (originalTreePath.isEmpty()) {
            return originalTreePath;
        }

        PathType inputPathType = getInputPathSchemas().get(0);
        PathType outputPathType = getOutputPathSchema();

        // Get all paths from original and filtered tree paths
        List<Path> originalPaths = originalTreePath.toList();
        List<Path> filteredPaths = filteredTreePath.isEmpty() ? new ArrayList<>() : filteredTreePath.toList();

        // Create a set of filtered path IDs for quick lookup
        Set<Long> filteredPathIds = new HashSet<>();
        for (Path filteredPath : filteredPaths) {
            filteredPathIds.add(filteredPath.getId());
        }

        // Merge paths: use filtered version if available, otherwise create null-filled version
        List<Path> mergedPaths = new ArrayList<>();
        for (Path originalPath : originalPaths) {
            if (filteredPathIds.contains(originalPath.getId())) {
                // Path passed filter - find the corresponding filtered path
                Path filteredPath = filteredPaths.stream()
                    .filter(p -> p.getId() == originalPath.getId())
                    .findFirst()
                    .orElse(null);
                if (filteredPath != null) {
                    mergedPaths.add(filteredPath);
                } else {
                    // Fallback: create aligned path with null values
                    Path alignedPath = SchemaUtil.alignToPathSchema(originalPath, inputPathType, outputPathType);
                    mergedPaths.add(alignedPath);
                }
            } else {
                // Path was filtered out - create aligned path with null values
                Path alignedPath = SchemaUtil.alignToPathSchema(originalPath, inputPathType, outputPathType);
                mergedPaths.add(alignedPath);
            }
        }

        // Convert merged paths back to tree path
        if (mergedPaths.isEmpty()) {
            return originalTreePath;
        } else if (mergedPaths.size() == 1) {
            return TreePaths.singletonPath(mergedPaths.get(0));
        } else {
            // For multiple paths, create a union tree path
            ITreePath result = TreePaths.singletonPath(mergedPaths.get(0));
            for (int i = 1; i < mergedPaths.size(); i++) {
                result = result.merge(TreePaths.singletonPath(mergedPaths.get(i)));
            }
            return result;
        }
    }

    /**
     * Create optional EdgeGroupRecord by merging original and filtered records.
     * Compare each path node and fill null for filtered-out positions.
     */
    private EdgeGroupRecord createOptionalEdgeGroupRecord(EdgeGroupRecord originalRecord, EdgeGroupRecord filterRecord) {
        // Compare original and filtered records node by node for each target ID
        Map<Object, ITreePath> resultPaths = new HashMap<>();

        for (Object targetId : originalRecord.getVertexIds()) {
            ITreePath originalTreePath = originalRecord.getPathById(targetId);
            ITreePath filteredTreePath = filterRecord.getPathById(targetId);

            // Always merge paths node by node, even if filter passed
            // This ensures filtered-out nodes are replaced with null values
            if (filteredTreePath == null) {
                filteredTreePath = originalTreePath.filter(path -> false, refPathIndices); // Empty tree path
            }

            ITreePath mergedTreePath = mergeTreePaths(originalTreePath, filteredTreePath);
            resultPaths.put(targetId, mergedTreePath);
        }

        // Return EdgeGroupRecord with original EdgeGroup but merged paths
        return EdgeGroupRecord.of(originalRecord.getEdgeGroup(), resultPaths);
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepFilterOperator(id, function);
    }
}
