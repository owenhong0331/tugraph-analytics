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

package com.antgroup.geaflow.dsl.rel.match;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.rel.MatchNodeVisitor;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * OptionalMatchFilter represents an optional filter operation in graph pattern matching.
 * Unlike regular MatchFilter, this filter will return null paths when the condition
 * is not satisfied, rather than filtering them out completely.
 */
public class OptionalMatchFilter extends MatchFilter {

    protected OptionalMatchFilter(RelOptCluster cluster, RelTraitSet traits,
                                  RelNode input, RexNode condition, PathRecordType pathType) {
        super(cluster, traits, input, condition, pathType);
    }

    @Override
    public OptionalMatchFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return copy(traitSet, input, condition, getPathSchema());
    }

    @Override
    public OptionalMatchFilter copy(RelTraitSet traitSet, RelNode input,
                                    RexNode condition, PathRecordType pathType) {
        return new OptionalMatchFilter(getCluster(), traitSet, input, condition, pathType);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return copy(traitSet, sole(inputs), getCondition(), pathSchema);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), getCondition());
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode newCondition = getCondition().accept(shuttle);
        return copy(traitSet, input, newCondition);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("optional", true);
    }

    public static OptionalMatchFilter create(RelNode input, RexNode condition, PathRecordType pathType) {
        return new OptionalMatchFilter(input.getCluster(), input.getTraitSet(), input, condition, pathType);
    }
}
