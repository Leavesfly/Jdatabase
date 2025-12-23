package com.jdatabase.parser;

import com.jdatabase.common.Types;
import com.jdatabase.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL解析器（递归下降解析）
 */
public class SQLParser {
    private final Lexer lexer;
    private Token currentToken;

    public SQLParser(String sql) {
        this.lexer = new Lexer(sql);
        this.currentToken = lexer.nextToken();
    }

    /**
     * 解析SQL语句
     */
    public Statement parse() {
        Statement stmt = parseStatement();
        expect(TokenType.EOF);
        return stmt;
    }

    private Statement parseStatement() {
        if (match(TokenType.CREATE)) {
            return parseCreateTable();
        } else if (match(TokenType.INSERT)) {
            return parseInsert();
        } else if (match(TokenType.SELECT)) {
            return parseSelect();
        } else if (match(TokenType.UPDATE)) {
            return parseUpdate();
        } else if (match(TokenType.DELETE)) {
            return parseDelete();
        } else {
            throw new RuntimeException("Unexpected token: " + currentToken);
        }
    }

    private CreateTableStatement parseCreateTable() {
        expect(TokenType.CREATE);
        expect(TokenType.TABLE);
        String tableName = expectIdentifier();
        expect(TokenType.LPAREN);
        
        List<CreateTableStatement.ColumnDefinition> columns = new ArrayList<>();
        String primaryKey = null;
        
        do {
            String colName = expectIdentifier();
            Types type = parseType();
            int length = -1;
            boolean nullable = true;
            boolean unique = false;
            
            if (type == Types.VARCHAR) {
                expect(TokenType.LPAREN);
                length = Integer.parseInt(expectNumber());
                expect(TokenType.RPAREN);
            }
            
            // 解析约束
            while (true) {
                if (match(TokenType.NOT)) {
                    expect(TokenType.NULL);
                    nullable = false;
                } else if (match(TokenType.NULL)) {
                    nullable = true;
                } else if (match(TokenType.IDENTIFIER) && currentToken.getValue().equalsIgnoreCase("UNIQUE")) {
                    unique = true;
                } else if (match(TokenType.IDENTIFIER) && currentToken.getValue().equalsIgnoreCase("PRIMARY")) {
                    expect(TokenType.IDENTIFIER); // KEY
                    primaryKey = colName;
                } else {
                    break;
                }
            }
            
            columns.add(new CreateTableStatement.ColumnDefinition(colName, type, length, nullable, unique));
        } while (match(TokenType.COMMA));
        
        expect(TokenType.RPAREN);
        
        CreateTableStatement stmt = new CreateTableStatement(tableName, columns);
        if (primaryKey != null) {
            stmt.setPrimaryKey(primaryKey);
        }
        return stmt;
    }

    private Types parseType() {
        if (match(TokenType.INT)) {
            return Types.INT;
        } else if (match(TokenType.LONG)) {
            return Types.LONG;
        } else if (match(TokenType.FLOAT)) {
            return Types.FLOAT;
        } else if (match(TokenType.DOUBLE)) {
            return Types.DOUBLE;
        } else if (match(TokenType.VARCHAR)) {
            return Types.VARCHAR;
        } else if (match(TokenType.BOOLEAN)) {
            return Types.BOOLEAN;
        } else {
            throw new RuntimeException("Expected type, got: " + currentToken);
        }
    }

    private InsertStatement parseInsert() {
        expect(TokenType.INSERT);
        expect(TokenType.INTO);
        String tableName = expectIdentifier();
        
        List<String> columnNames = null;
        if (match(TokenType.LPAREN)) {
            columnNames = new ArrayList<>();
            do {
                columnNames.add(expectIdentifier());
            } while (match(TokenType.COMMA));
            expect(TokenType.RPAREN);
        }
        
        expect(TokenType.VALUES);
        List<List<Expression>> valuesList = new ArrayList<>();
        
        do {
            expect(TokenType.LPAREN);
            List<Expression> values = new ArrayList<>();
            do {
                values.add(parseExpression());
            } while (match(TokenType.COMMA));
            expect(TokenType.RPAREN);
            valuesList.add(values);
        } while (match(TokenType.COMMA));
        
        return new InsertStatement(tableName, columnNames, valuesList);
    }

    private SelectStatement parseSelect() {
        expect(TokenType.SELECT);
        
        List<SelectStatement.SelectItem> selectItems = new ArrayList<>();
        
        if (match(TokenType.IDENTIFIER) && currentToken.getValue().equalsIgnoreCase("DISTINCT")) {
            // DISTINCT关键字已处理
        } else if (currentToken.getType() != TokenType.IDENTIFIER && 
                   currentToken.getType() != TokenType.MULTIPLY) {
            // 回退
            currentToken = new Token(TokenType.IDENTIFIER, "DISTINCT", 0, 0);
        }
        
        do {
            Expression expr = parseExpression();
            String alias = null;
            if (match(TokenType.AS)) {
                alias = expectIdentifier();
            } else if (match(TokenType.IDENTIFIER) && 
                      !currentToken.getValue().equalsIgnoreCase("FROM")) {
                alias = currentToken.getValue();
            }
            selectItems.add(new SelectStatement.SelectItem(expr, alias));
        } while (match(TokenType.COMMA));
        
        expect(TokenType.FROM);
        List<SelectStatement.TableReference> fromClause = parseFromClause();
        
        SelectStatement stmt = new SelectStatement(selectItems, fromClause);
        
        if (match(TokenType.WHERE)) {
            stmt.setWhereClause(parseExpression());
        }
        
        if (match(TokenType.GROUP)) {
            expect(TokenType.BY);
            List<Expression> groupBy = new ArrayList<>();
            do {
                groupBy.add(parseExpression());
            } while (match(TokenType.COMMA));
            stmt.setGroupByClause(groupBy);
            
            if (match(TokenType.HAVING)) {
                stmt.setHavingClause(parseExpression());
            }
        }
        
        if (match(TokenType.ORDER)) {
            expect(TokenType.BY);
            List<SelectStatement.OrderByItem> orderBy = new ArrayList<>();
            do {
                Expression expr = parseExpression();
                boolean ascending = true;
                if (match(TokenType.IDENTIFIER)) {
                    String dir = currentToken.getValue().toUpperCase();
                    if (dir.equals("DESC")) {
                        ascending = false;
                    } else if (!dir.equals("ASC")) {
                        // 回退
                        currentToken = new Token(TokenType.IDENTIFIER, currentToken.getValue(), 
                                               currentToken.getLine(), currentToken.getColumn());
                    }
                }
                orderBy.add(new SelectStatement.OrderByItem(expr, ascending));
            } while (match(TokenType.COMMA));
            stmt.setOrderByClause(orderBy);
        }
        
        return stmt;
    }

    private List<SelectStatement.TableReference> parseFromClause() {
        List<SelectStatement.TableReference> tables = new ArrayList<>();
        
        String tableName = expectIdentifier();
        String alias = null;
        if (match(TokenType.IDENTIFIER) || 
            (match(TokenType.AS) && (currentToken = lexer.nextToken()).getType() == TokenType.IDENTIFIER)) {
            alias = currentToken.getValue();
            currentToken = lexer.nextToken();
        }
        
        tables.add(new SelectStatement.TableReference(tableName, alias));
        
        while (match(TokenType.COMMA) || 
               match(TokenType.INNER) || 
               match(TokenType.LEFT) || 
               match(TokenType.RIGHT)) {
            SelectStatement.JoinType joinType = SelectStatement.JoinType.INNER;
            
            if (currentToken.getType() == TokenType.LEFT) {
                joinType = SelectStatement.JoinType.LEFT;
                expect(TokenType.LEFT);
            } else if (currentToken.getType() == TokenType.RIGHT) {
                joinType = SelectStatement.JoinType.RIGHT;
                expect(TokenType.RIGHT);
            } else if (currentToken.getType() == TokenType.INNER) {
                expect(TokenType.INNER);
            }
            
            expect(TokenType.JOIN);
            String joinTableName = expectIdentifier();
            String joinAlias = null;
            if (match(TokenType.IDENTIFIER) || 
                (match(TokenType.AS) && (currentToken = lexer.nextToken()).getType() == TokenType.IDENTIFIER)) {
                joinAlias = currentToken.getValue();
                currentToken = lexer.nextToken();
            }
            
            expect(TokenType.ON);
            Expression joinCondition = parseExpression();
            
            tables.add(new SelectStatement.TableReference(joinTableName, joinAlias, joinType, joinCondition));
        }
        
        return tables;
    }

    private UpdateStatement parseUpdate() {
        expect(TokenType.UPDATE);
        String tableName = expectIdentifier();
        expect(TokenType.SET);
        
        List<UpdateStatement.Assignment> assignments = new ArrayList<>();
        do {
            String columnName = expectIdentifier();
            expect(TokenType.EQ);
            Expression value = parseExpression();
            assignments.add(new UpdateStatement.Assignment(columnName, value));
        } while (match(TokenType.COMMA));
        
        UpdateStatement stmt = new UpdateStatement(tableName, assignments);
        
        if (match(TokenType.WHERE)) {
            stmt.setWhereClause(parseExpression());
        }
        
        return stmt;
    }

    private DeleteStatement parseDelete() {
        expect(TokenType.DELETE);
        expect(TokenType.FROM);
        String tableName = expectIdentifier();
        
        DeleteStatement stmt = new DeleteStatement(tableName);
        
        if (match(TokenType.WHERE)) {
            stmt.setWhereClause(parseExpression());
        }
        
        return stmt;
    }

    private Expression parseExpression() {
        return parseOrExpression();
    }

    private Expression parseOrExpression() {
        Expression left = parseAndExpression();
        while (match(TokenType.OR)) {
            Expression right = parseAndExpression();
            left = new Expression.BinaryExpression(left, "OR", right);
        }
        return left;
    }

    private Expression parseAndExpression() {
        Expression left = parseComparison();
        while (match(TokenType.AND)) {
            Expression right = parseComparison();
            left = new Expression.BinaryExpression(left, "AND", right);
        }
        return left;
    }

    private Expression parseComparison() {
        Expression left = parseAdditive();
        if (match(TokenType.EQ) || match(TokenType.NE) || 
            match(TokenType.LT) || match(TokenType.LE) || 
            match(TokenType.GT) || match(TokenType.GE)) {
            String op = currentToken.getValue();
            Expression right = parseAdditive();
            return new Expression.BinaryExpression(left, op, right);
        } else if (match(TokenType.IS)) {
            boolean notNull = false;
            if (match(TokenType.NOT)) {
                notNull = true;
            }
            expect(TokenType.NULL);
            Expression right = new Expression.Literal(null, Types.VARCHAR);
            return new Expression.BinaryExpression(left, notNull ? "IS NOT NULL" : "IS NULL", right);
        }
        return left;
    }

    private Expression parseAdditive() {
        Expression left = parseMultiplicative();
        while (match(TokenType.PLUS) || match(TokenType.MINUS)) {
            String op = currentToken.getValue();
            Expression right = parseMultiplicative();
            left = new Expression.BinaryExpression(left, op, right);
        }
        return left;
    }

    private Expression parseMultiplicative() {
        Expression left = parseUnary();
        while (match(TokenType.MULTIPLY) || match(TokenType.DIVIDE)) {
            String op = currentToken.getValue();
            Expression right = parseUnary();
            left = new Expression.BinaryExpression(left, op, right);
        }
        return left;
    }

    private Expression parseUnary() {
        if (match(TokenType.MINUS)) {
            return new Expression.UnaryExpression("-", parseUnary());
        } else if (match(TokenType.NOT)) {
            return new Expression.UnaryExpression("NOT", parseUnary());
        }
        return parsePrimary();
    }

    private Expression parsePrimary() {
        if (match(TokenType.NUMBER)) {
            String num = currentToken.getValue();
            if (num.contains(".")) {
                return new Expression.Literal(Double.parseDouble(num), Types.DOUBLE);
            } else {
                return new Expression.Literal(Long.parseLong(num), Types.LONG);
            }
        } else if (match(TokenType.STRING)) {
            return new Expression.Literal(currentToken.getValue(), Types.VARCHAR);
        } else if (match(TokenType.LPAREN)) {
            Expression expr = parseExpression();
            expect(TokenType.RPAREN);
            return expr;
        } else if (match(TokenType.COUNT) || match(TokenType.SUM) || 
                   match(TokenType.AVG) || match(TokenType.MAX) || match(TokenType.MIN)) {
            String funcName = currentToken.getValue().toUpperCase();
            boolean distinct = false;
            if (match(TokenType.LPAREN)) {
                if (match(TokenType.IDENTIFIER) && currentToken.getValue().equalsIgnoreCase("DISTINCT")) {
                    distinct = true;
                } else {
                    // 回退
                    currentToken = new Token(TokenType.IDENTIFIER, currentToken.getValue(), 
                                           currentToken.getLine(), currentToken.getColumn());
                }
                List<Expression> args = new ArrayList<>();
                if (currentToken.getType() != TokenType.RPAREN) {
                    do {
                        args.add(parseExpression());
                    } while (match(TokenType.COMMA));
                }
                expect(TokenType.RPAREN);
                return new Expression.FunctionCall(funcName, args, distinct);
            }
        } else if (match(TokenType.IDENTIFIER)) {
            String identifier = currentToken.getValue();
            if (match(TokenType.DOT)) {
                String columnName = expectIdentifier();
                return new Expression.ColumnReference(identifier, columnName);
            } else {
                // 可能是列名或表别名
                return new Expression.ColumnReference(null, identifier);
            }
        } else if (match(TokenType.MULTIPLY)) {
            return new Expression.ColumnReference(null, "*");
        }
        
        throw new RuntimeException("Unexpected token in expression: " + currentToken);
    }

    private boolean match(TokenType type) {
        if (currentToken.getType() == type) {
            currentToken = lexer.nextToken();
            return true;
        }
        return false;
    }

    private String expectIdentifier() {
        if (currentToken.getType() != TokenType.IDENTIFIER) {
            throw new RuntimeException("Expected identifier, got: " + currentToken);
        }
        String value = currentToken.getValue();
        currentToken = lexer.nextToken();
        return value;
    }

    private String expectNumber() {
        if (currentToken.getType() != TokenType.NUMBER) {
            throw new RuntimeException("Expected number, got: " + currentToken);
        }
        String value = currentToken.getValue();
        currentToken = lexer.nextToken();
        return value;
    }

    private void expect(TokenType type) {
        if (!match(type)) {
            throw new RuntimeException("Expected " + type + ", got: " + currentToken);
        }
    }
}

