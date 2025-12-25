package com.jdatabase.executor;

import com.jdatabase.common.Schema;
import com.jdatabase.common.Tuple;
import com.jdatabase.common.Value;
import com.jdatabase.index.IndexManager;
import com.jdatabase.optimizer.QueryOptimizer;
import com.jdatabase.parser.ast.*;
import com.jdatabase.storage.StorageManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 查询执行器
 */
public class QueryExecutor {
    private final StorageManager storageManager;
    private final IndexManager indexManager;
    private final QueryOptimizer queryOptimizer;

    public QueryExecutor(StorageManager storageManager, IndexManager indexManager, QueryOptimizer queryOptimizer) {
        this.storageManager = storageManager;
        this.indexManager = indexManager;
        this.queryOptimizer = queryOptimizer;
    }

    /**
     * 执行SELECT语句
     */
    public List<Tuple> executeSelect(SelectStatement stmt) throws IOException {
        // 查询优化：在构建执行计划前先优化查询
        SelectStatement optimizedStmt = queryOptimizer.optimize(stmt);
        
        Operator root = buildExecutionPlan(optimizedStmt);
        
        root.open();
        List<Tuple> results = new ArrayList<>();
        while (root.hasNext()) {
            Tuple tuple = root.next();
            if (tuple != null) {
                results.add(tuple);
            }
        }
        root.close();
        
        return results;
    }

    /**
     * 执行INSERT语句
     */
    public int executeInsert(InsertStatement stmt) throws IOException {
        String tableName = stmt.getTableName();
        Schema schema = storageManager.getCatalog().getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }

        int count = 0;
        for (List<Expression> values : stmt.getValuesList()) {
            Tuple tuple = new Tuple(schema);
            
            List<String> columnNames = stmt.getColumnNames();
            if (columnNames.isEmpty()) {
                // 插入所有列
                for (int i = 0; i < values.size() && i < schema.getColumnCount(); i++) {
                    Value value = evaluateExpression(values.get(i), null);
                    tuple.setValue(i, value);
                }
            } else {
                // 插入指定列
                for (int i = 0; i < columnNames.size() && i < values.size(); i++) {
                    int colIndex = schema.getColumnIndex(columnNames.get(i));
                    if (colIndex >= 0) {
                        Value value = evaluateExpression(values.get(i), null);
                        tuple.setValue(colIndex, value);
                    }
                }
            }
            
            storageManager.insertTuple(tableName, tuple);
            count++;
        }
        
        return count;
    }

    /**
     * 执行UPDATE语句
     */
    public int executeUpdate(UpdateStatement stmt) throws IOException {
        String tableName = stmt.getTableName();
        Schema schema = storageManager.getCatalog().getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }

        // 扫描表并应用WHERE条件
        List<Tuple> tuples = storageManager.scanTable(tableName);
        int count = 0;
        
        for (Tuple tuple : tuples) {
            if (stmt.getWhereClause() == null || 
                evaluateCondition(stmt.getWhereClause(), tuple)) {
                // 更新元组
                for (UpdateStatement.Assignment assignment : stmt.getAssignments()) {
                    int colIndex = schema.getColumnIndex(assignment.getColumnName());
                    if (colIndex >= 0) {
                        Value newValue = evaluateExpression(assignment.getValue(), tuple);
                        tuple.setValue(colIndex, newValue);
                    }
                }
                // 注意：这里需要RecordId来更新，简化处理
                count++;
            }
        }
        
        return count;
    }

    /**
     * 执行DELETE语句
     */
    public int executeDelete(DeleteStatement stmt) throws IOException {
        String tableName = stmt.getTableName();
        Schema schema = storageManager.getCatalog().getSchema(tableName);
        if (schema == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }

        // 扫描表并应用WHERE条件
        List<Tuple> tuples = storageManager.scanTable(tableName);
        int count = 0;
        
        for (Tuple tuple : tuples) {
            if (stmt.getWhereClause() == null || 
                evaluateCondition(stmt.getWhereClause(), tuple)) {
                // 注意：这里需要RecordId来删除，简化处理
                count++;
            }
        }
        
        return count;
    }

    /**
     * 构建执行计划
     */
    private Operator buildExecutionPlan(SelectStatement stmt) {
        // 构建FROM子句的扫描操作符
        List<SelectStatement.TableReference> tables = stmt.getFromClause();
        Operator root = null;
        
        for (int i = 0; i < tables.size(); i++) {
            SelectStatement.TableReference tableRef = tables.get(i);
            Schema schema = storageManager.getCatalog().getSchema(tableRef.getTableName());
            if (schema == null) {
                throw new RuntimeException("Table not found: " + tableRef.getTableName());
            }
            
            // 尝试使用索引优化扫描
            Operator scan = buildScanOperator(tableRef.getTableName(), schema, stmt.getWhereClause());
            
            if (i == 0) {
                root = scan;
            } else {
                // JOIN
                SelectStatement.JoinType joinType = tableRef.getJoinType();
                root = new JoinOperator(root, scan, tableRef.getJoinCondition(), joinType);
            }
        }
        
        // WHERE子句（如果还没有在索引扫描中处理）
        if (stmt.getWhereClause() != null && !isIndexScanUsed(root)) {
            root = new FilterOperator(root, stmt.getWhereClause());
        }
        
        // GROUP BY + 聚合
        if (stmt.getGroupByClause() != null || hasAggregateFunctions(stmt.getSelectItems())) {
            root = new AggregateOperator(root, stmt.getSelectItems(), 
                                        stmt.getGroupByClause(), stmt.getHavingClause());
        }
        
        // SELECT投影
        root = new ProjectOperator(root, stmt.getSelectItems());
        
        // ORDER BY
        if (stmt.getOrderByClause() != null) {
            root = new SortOperator(root, stmt.getOrderByClause());
        }
        
        return root;
    }

    private boolean hasAggregateFunctions(List<SelectStatement.SelectItem> selectItems) {
        for (SelectStatement.SelectItem item : selectItems) {
            if (item.getExpression() instanceof Expression.FunctionCall) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateCondition(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Object left = evaluateExpression(binExpr.getLeft(), tuple);
            Object right = evaluateExpression(binExpr.getRight(), tuple);
            return compareValues(left, binExpr.getOperator(), right);
        } else if (expr instanceof Expression.UnaryExpression) {
            Expression.UnaryExpression unaryExpr = (Expression.UnaryExpression) expr;
            boolean result = evaluateCondition(unaryExpr.getOperand(), tuple);
            return unaryExpr.getOperator().equals("NOT") ? !result : result;
        }
        return true;
    }

    private Value evaluateExpression(Expression expr, Tuple tuple) {
        if (expr instanceof Expression.Literal) {
            Expression.Literal literal = (Expression.Literal) expr;
            return new Value(literal.getType(), literal.getValue());
        } else if (expr instanceof Expression.ColumnReference) {
            Expression.ColumnReference colRef = (Expression.ColumnReference) expr;
            if (tuple != null) {
                return tuple.getValue(colRef.getColumnName());
            }
        }
        return null;
    }

    private boolean compareValues(Object left, String op, Object right) {
        if (op.equals("IS NULL")) {
            return left == null;
        } else if (op.equals("IS NOT NULL")) {
            return left != null;
        }

        if (left == null || right == null) {
            return false;
        }

        int cmp = 0;
        if (left instanceof Comparable && right instanceof Comparable) {
            @SuppressWarnings("unchecked")
            Comparable<Object> leftComp = (Comparable<Object>) left;
            cmp = leftComp.compareTo(right);
        }

        switch (op) {
            case "=":
            case "==":
                return cmp == 0;
            case "!=":
            case "<>":
                return cmp != 0;
            case "<":
                return cmp < 0;
            case "<=":
                return cmp <= 0;
            case ">":
                return cmp > 0;
            case ">=":
                return cmp >= 0;
            case "AND":
                return (left instanceof Boolean && (Boolean) left) && 
                       (right instanceof Boolean && (Boolean) right);
            case "OR":
                return (left instanceof Boolean && (Boolean) left) || 
                       (right instanceof Boolean && (Boolean) right);
            default:
                return false;
        }
    }

    /**
     * 构建扫描操作符，尝试使用索引优化
     */
    private Operator buildScanOperator(String tableName, Schema schema, Expression whereClause) {
        // 检查 WHERE 条件中是否有可以使用索引的等值查询
        if (whereClause != null) {
            IndexScanInfo indexInfo = findIndexableCondition(whereClause, tableName, schema);
            if (indexInfo != null) {
                return new IndexScanOperator(storageManager, indexManager, 
                    tableName, schema, indexInfo.columnName, indexInfo.searchKey);
            }
        }
        
        // 默认使用顺序扫描
        return new SeqScanOperator(storageManager, tableName, schema);
    }

    /**
     * 查找可以使用索引的条件
     */
    private IndexScanInfo findIndexableCondition(Expression expr, String tableName, Schema schema) {
        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            String op = binExpr.getOperator();
            
            // 只支持等值查询使用索引
            if (op.equals("=") || op.equals("==")) {
                Expression left = binExpr.getLeft();
                Expression right = binExpr.getRight();
                
                // 检查是否是列 = 常量 的形式
                String columnName = null;
                Comparable<?> searchKey = null;
                
                if (left instanceof Expression.ColumnReference && right instanceof Expression.Literal) {
                    Expression.ColumnReference colRef = (Expression.ColumnReference) left;
                    Expression.Literal literal = (Expression.Literal) right;
                    
                    // 检查列是否属于当前表
                    if (colRef.getTableName() == null || colRef.getTableName().equals(tableName)) {
                        columnName = colRef.getColumnName();
                        Object value = literal.getValue();
                        if (value instanceof Comparable) {
                            searchKey = (Comparable<?>) value;
                        }
                    }
                } else if (right instanceof Expression.ColumnReference && left instanceof Expression.Literal) {
                    Expression.ColumnReference colRef = (Expression.ColumnReference) right;
                    Expression.Literal literal = (Expression.Literal) left;
                    
                    if (colRef.getTableName() == null || colRef.getTableName().equals(tableName)) {
                        columnName = colRef.getColumnName();
                        Object value = literal.getValue();
                        if (value instanceof Comparable) {
                            searchKey = (Comparable<?>) value;
                        }
                    }
                }
                
                // 检查索引是否存在
                if (columnName != null && searchKey != null) {
                    Set<String> indexedColumns = storageManager.getCatalog().getIndexedColumns(tableName);
                    if (indexedColumns.contains(columnName)) {
                        return new IndexScanInfo(columnName, searchKey);
                    }
                }
            }
        }
        
        return null;
    }

    /**
     * 检查是否使用了索引扫描
     */
    private boolean isIndexScanUsed(Operator operator) {
        return operator instanceof IndexScanOperator;
    }

    /**
     * 索引扫描信息
     */
    private static class IndexScanInfo {
        final String columnName;
        final Comparable<?> searchKey;

        IndexScanInfo(String columnName, Comparable<?> searchKey) {
            this.columnName = columnName;
            this.searchKey = searchKey;
        }
    }
}

