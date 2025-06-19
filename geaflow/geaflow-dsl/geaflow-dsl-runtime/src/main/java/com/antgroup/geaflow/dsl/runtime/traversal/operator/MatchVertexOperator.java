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

import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.common.data.VirtualId;
import com.antgroup.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Histogram;
import java.util.Set;

public class MatchVertexOperator extends AbstractStepOperator<MatchVertexFunction, StepRecord,
    VertexRecord> implements LabeledStepOperator {

    private Histogram loadVertexRt;

    private final boolean isOptionMatch;

    private Set<Object> idSet;

    public MatchVertexOperator(long id, MatchVertexFunction function) {
        super(id, function);
        if (function instanceof MatchVertexFunctionImpl) {
            isOptionMatch = ((MatchVertexFunctionImpl) function).isOptionalMatchVertex();
            idSet = ((MatchVertexFunctionImpl) function).getIdSet();
        } else {
            isOptionMatch = false;
        }
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        loadVertexRt = metricGroup.histogram(MetricNameFormatter.loadVertexTimeRtName(getName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.VERTEX) {
            processVertex((VertexRecord) record);
        } else {
            EdgeGroupRecord edgeGroupRecord = (EdgeGroupRecord) record;
            processEdgeGroup(edgeGroupRecord);
        }
    }

    private void processVertex(VertexRecord vertexRecord) {
        RowVertex vertex = vertexRecord.getVertex();
        if (vertex instanceof IdOnlyVertex && needLoadVertex(vertex.getId())) {
            long startTs = System.currentTimeMillis();
            vertex = context.loadVertex(vertex.getId(),
                function.getVertexFilter(),
                graphSchema,
                addingVertexFieldTypes);
            loadVertexRt.update(System.currentTimeMillis() - startTs);
            if (vertex == null && !isOptionMatch) {
                // load a non-exists vertex, just skip.
                return;
            }
        }

        if (vertex != null) {
            if (!function.getVertexTypes().isEmpty()
                && !function.getVertexTypes().contains(vertex.getBinaryLabel())) {
                // filter by the vertex types.
                return;
            }
            if (!idSet.isEmpty() && !idSet.contains(vertex.getId())) {
                return;
            }
            vertex = alignToOutputSchema(vertex);
        }

        ITreePath currentPath;
        if (needAddToPath) {
            currentPath = vertexRecord.getTreePath().extendTo(vertex);
        } else {
            currentPath = vertexRecord.getTreePath();
        }
        if (vertex == null) {
            vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
        }
        System.out.print("coll23424ec23424t");
        collect(VertexRecord.of(vertex, currentPath),isOptionMatch);
    }

    private void processEdgeGroup(EdgeGroupRecord edgeGroupRecord) {
        EdgeGroup edgeGroup = edgeGroupRecord.getEdgeGroup();

        // For OPTIONAL MATCH: process all vertex IDs (including null for no-edge cases)
        // For regular MATCH: only process edges that exist
        if (isOptionMatch) {
            // Process all target vertex IDs from the EdgeGroupRecord
            for (Object targetId : edgeGroupRecord.getVertexIds()) {
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                if (targetId == null) {
                    // This is the case where no edges were found - create null vertex
                    RowVertex vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
                    context.setVertex(vertex);
                    processVertex(VertexRecord.of(null, treePath));
                } else {
                    // Load the actual target vertex
                    RowVertex vertex = context.loadVertex(targetId, function.getVertexFilter(), graphSchema, addingVertexFieldTypes);
                    if (vertex != null) {
                        context.setVertex(vertex);
                        processVertex(VertexRecord.of(vertex, treePath));
                    } else {
                        // Target vertex doesn't exist - create null vertex for optional match
                        vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
                        context.setVertex(vertex);
                        processVertex(VertexRecord.of(null, treePath));
                    }
                }
            }
        } else {
            // Regular MATCH: only process existing edges
            for (RowEdge edge : edgeGroup) {
                Object targetId = edge.getTargetId();
                // load targetId.
                RowVertex vertex = context.loadVertex(targetId, function.getVertexFilter(), graphSchema, addingVertexFieldTypes);
                if (vertex != null) {
                    ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                    // set current vertex.
                    context.setVertex(vertex);
                    // process new vertex.
                    processVertex(VertexRecord.of(vertex, treePath));
                }
            }
        }
    }

    private boolean needLoadVertex(Object vertexId) {
        // skip load virtual id.
        return !(vertexId instanceof VirtualId);
    }

    @Override
    public void close() {

    }

    @Override
    public StepOperator<StepRecord, VertexRecord> copyInternal() {
        return new MatchVertexOperator(id, function);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
