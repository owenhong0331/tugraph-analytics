/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.antgroup.geaflow.dsl.sqlnode;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator; // Specific import for SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * SqlOptionalMatchPattern 表示 GQL 中的 OPTIONAL MATCH 子句。
 * 它类似于 MATCH，但执行的是左外连接，对于不匹配的模式返回 null，而不是过滤掉它们。
 */
public class SqlOptionalMatchPattern extends SqlCall {

    // 定义 OPTIONAL MATCH 的操作符。
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("OPTIONAL MATCH", SqlKind.OTHER);

    // 与匹配模式关联的 'FROM' 子句（如果有）。
    private SqlNode from;
    // 图中要匹配的路径模式列表（例如：(a)-[r]->(b)）。
    private SqlNodeList pathPatterns;
    // 用于过滤匹配模式的 'WHERE' 子句。
    private SqlNode where;
    // 用于对结果排序的 'ORDER BY' 子句。
    private SqlNodeList orderBy;
    // 用于限制结果数量的 'LIMIT' 子句。
    private SqlNode limit;

    /**
     * 构造一个 SqlOptionalMatchPattern 实例。
     *
     * @param pos 解析器位置。
     * @param from FROM 子句。
     * @param pathPatterns 路径模式列表。
     * @param where WHERE 子句。
     * @param orderBy ORDER BY 子句。
     * @param limit LIMIT 子句。
     */
    public SqlOptionalMatchPattern(SqlParserPos pos, SqlNode from, SqlNodeList pathPatterns,
                                   SqlNode where, SqlNodeList orderBy, SqlNode limit) {
        super(pos);
        this.from = from;
        // 根据初始框架，pathPatterns 是必需的。
        this.pathPatterns = Objects.requireNonNull(pathPatterns, "Path patterns cannot be null for OPTIONAL MATCH.");
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    /**
     * 获取此 SQL 调用的操作符。
     * @return SqlOperator。
     */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /**
     * 获取此 SQL 调用的操作数列表。
     * 操作数的顺序对于 setOperand 和 unparse 等方法至关重要。
     * @return SqlNode 操作数列表。
     */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(getFrom(), getPathPatterns(), getWhere(),
            getOrderBy(), getLimit());
    }

    /**
     * 获取此表达式的 SQL 类型。
     * OPTIONAL MATCH 是图查询匹配模式的一种变体。
     * @return SqlKind。
     */
    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_PATTERN;
    }

    /**
     * 根据验证器和作用域验证此 SQL 调用。
     * @param validator SQL 验证器。
     * @param scope 当前验证作用域。
     */
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        // 将验证委托给 Calcite 验证器。
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    /**
     * 将此 SQL 调用反解析（转换为 SQL 字符串）。
     * @param writer 要写入的 SQL 写入器。
     * @param leftPrec 左优先级。
     * @param rightPrec 右优先级。
     */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("OPTIONAL MATCH");
        // 反解析每个路径模式。
        if (pathPatterns != null) { // 防御性检查，尽管构造函数使其非空。
            for (int i = 0; i < pathPatterns.size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }
                pathPatterns.get(i).unparse(writer, leftPrec, rightPrec);
                writer.newlineAndIndent(); // 为了可读性换行和缩进，特别是当有多个模式时。
            }
        }

        // 如果存在，反解析 WHERE 子句。
        if (where != null) {
            writer.keyword("WHERE");
            where.unparse(writer, 0, 0); // WHERE 子句不需要优先级。
        }

        // 如果存在，反解析 ORDER BY 子句。
        if (orderBy != null && orderBy.size() > 0) {
            writer.keyword("ORDER BY");
            for (int i = 0; i < orderBy.size(); i++) {
                SqlNode orderItem = orderBy.get(i);
                if (i > 0) {
                    writer.print(",");
                }
                orderItem.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
        }

        // 如果存在，反解析 LIMIT 子句。
        if (limit != null) {
            writer.keyword("LIMIT");
            limit.unparse(writer, leftPrec, rightPrec);
        }
    }

    /**
     * 在特定索引处设置操作数。
     * 这由 Calcite 的内部重写机制使用。
     * @param i 要设置的操作数的索引。
     * @param operand 要设置的 SqlNode。
     * @throws IllegalArgumentException 如果索引超出范围。
     */
    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0: // 对应 'from'
                this.from = operand;
                break;
            case 1: // 对应 'pathPatterns'
                this.pathPatterns = (SqlNodeList) operand;
                break;
            case 2: // 对应 'where'
                this.where = operand;
                break;
            case 3: // 对应 'orderBy'
                this.orderBy = (SqlNodeList) operand;
                break;
            case 4: // 对应 'limit'
                this.limit = operand;
                break;
            default:
                throw new IllegalArgumentException("SqlOptionalMatchPattern 的非法索引: " + i);
        }
    }

    // --- 字段的 Getter 方法 ---

    public SqlNode getFrom() {
        return from;
    }

    public SqlNodeList getPathPatterns() {
        return pathPatterns;
    }

    public SqlNode getWhere() {
        return where;
    }

    public SqlNodeList getOrderBy() {
        return orderBy;
    }

    public SqlNode getLimit() {
        return limit;
    }

    // --- 字段的 Setter 方法 (主要用于 Calcite 内部转换) ---

    public void setFrom(SqlNode from) {
        this.from = from;
    }

    // pathPatterns 没有 setter，因为它通常在构造函数中设置并通过 setOperand 进行设置。
    // public void setPathPatterns(SqlNodeList pathPatterns) { this.pathPatterns = pathPatterns; }

    public void setWhere(SqlNode where) {
        this.where = where;
    }

    public void setOrderBy(SqlNodeList orderBy) {
        this.orderBy = orderBy;
    }

    public void setLimit(SqlNode limit) {
        this.limit = limit;
    }

    /**
     * 检查此 OPTIONAL MATCH 模式是否由单个路径模式组成。
     * @return 如果是单个路径模式，则为 true；否则为 false。
     */
    public boolean isSinglePattern() {
        // 假设 SqlPathPattern 是单个路径的特定节点类型。
        return pathPatterns.size() == 1 && pathPatterns.get(0) instanceof SqlPathPattern;
    }

    /**
     * 指示查询是否是 DISTINCT 的。对于 MATCH/OPTIONAL MATCH，
     * DISTINCT 通常在随后的 SELECT 子句中处理，而不是在此处。
     * @return 始终返回 false。
     */
    public final boolean isDistinct() {
        return false;
    }
}

// 注意：SqlPathPattern 是一个假定的类，表示一个单独的图路径模式（例如：(a)-[r]->(b)）。
// 它需要在 'com.antgroup.geaflow.dsl.sqlnode' 包中的其他地方定义。
