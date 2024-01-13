// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * OptExprBuilder is used to build OptExpression tree
 */
public class OptExprBuilder {

    /* 当前节点算子，分为 LogicalOperator 和 PhysicalOperator */
    private final Operator root;

    /* 子节点算子，可以是 0 个，也可以是多个 */
    private final List<OptExprBuilder> inputs;

    private ExpressionMapping expressionMapping;

    public OptExprBuilder(Operator root, List<OptExprBuilder> inputs, ExpressionMapping expressionMapping) {
        this.root = root;
        this.inputs = inputs;
        this.expressionMapping = expressionMapping;
    }

    public Scope getScope() {
        return expressionMapping.getScope();
    }

    public List<ColumnRefOperator> getFieldMappings() {
        return expressionMapping.getFieldMappings();
    }

    public ExpressionMapping getExpressionMapping() {
        return expressionMapping;
    }

    public void setExpressionMapping(ExpressionMapping expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    public OptExpression getRoot() {
        if (inputs.isEmpty()) {
            return new OptExpression(root);
        } else {
            return OptExpression
                    .create(root, inputs.stream()
                            .map(OptExprBuilder::getRoot)
                            .collect(Collectors.toList())
                    );
        }
    }

    public List<OptExprBuilder> getInputs() {
        return inputs;
    }

    public void addChild(OptExprBuilder builder) {
        inputs.add(builder);
    }

    public OptExprBuilder withNewRoot(Operator operator) {
        return new OptExprBuilder(operator, Collections.singletonList(this), expressionMapping);
    }
}