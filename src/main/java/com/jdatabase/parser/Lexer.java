package com.jdatabase.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * 词法分析器
 */
public class Lexer {
    private final String input;
    private int position;
    private int line;
    private int column;
    
    private static final Map<String, TokenType> KEYWORDS = new HashMap<>();
    
    static {
        KEYWORDS.put("CREATE", TokenType.CREATE);
        KEYWORDS.put("TABLE", TokenType.TABLE);
        KEYWORDS.put("INSERT", TokenType.INSERT);
        KEYWORDS.put("INTO", TokenType.INTO);
        KEYWORDS.put("VALUES", TokenType.VALUES);
        KEYWORDS.put("SELECT", TokenType.SELECT);
        KEYWORDS.put("FROM", TokenType.FROM);
        KEYWORDS.put("WHERE", TokenType.WHERE);
        KEYWORDS.put("UPDATE", TokenType.UPDATE);
        KEYWORDS.put("SET", TokenType.SET);
        KEYWORDS.put("DELETE", TokenType.DELETE);
        KEYWORDS.put("JOIN", TokenType.JOIN);
        KEYWORDS.put("ON", TokenType.ON);
        KEYWORDS.put("INNER", TokenType.INNER);
        KEYWORDS.put("LEFT", TokenType.LEFT);
        KEYWORDS.put("RIGHT", TokenType.RIGHT);
        KEYWORDS.put("ORDER", TokenType.ORDER);
        KEYWORDS.put("BY", TokenType.BY);
        KEYWORDS.put("GROUP", TokenType.GROUP);
        KEYWORDS.put("HAVING", TokenType.HAVING);
        KEYWORDS.put("AS", TokenType.AS);
        KEYWORDS.put("AND", TokenType.AND);
        KEYWORDS.put("OR", TokenType.OR);
        KEYWORDS.put("NOT", TokenType.NOT);
        KEYWORDS.put("NULL", TokenType.NULL);
        KEYWORDS.put("IS", TokenType.IS);
        KEYWORDS.put("INT", TokenType.INT);
        KEYWORDS.put("LONG", TokenType.LONG);
        KEYWORDS.put("FLOAT", TokenType.FLOAT);
        KEYWORDS.put("DOUBLE", TokenType.DOUBLE);
        KEYWORDS.put("VARCHAR", TokenType.VARCHAR);
        KEYWORDS.put("BOOLEAN", TokenType.BOOLEAN);
        KEYWORDS.put("COUNT", TokenType.COUNT);
        KEYWORDS.put("SUM", TokenType.SUM);
        KEYWORDS.put("AVG", TokenType.AVG);
        KEYWORDS.put("MAX", TokenType.MAX);
        KEYWORDS.put("MIN", TokenType.MIN);
    }

    public Lexer(String input) {
        this.input = input;
        this.position = 0;
        this.line = 1;
        this.column = 1;
    }

    public Token nextToken() {
        skipWhitespace();
        
        if (position >= input.length()) {
            return new Token(TokenType.EOF, "", line, column);
        }
        
        char ch = input.charAt(position);
        
        // 数字
        if (Character.isDigit(ch)) {
            return readNumber();
        }
        
        // 字符串
        if (ch == '\'' || ch == '"') {
            return readString();
        }
        
        // 标识符或关键字
        if (Character.isLetter(ch) || ch == '_') {
            return readIdentifier();
        }
        
        // 运算符和分隔符
        return readOperatorOrDelimiter();
    }

    private Token readNumber() {
        int start = position;
        int startColumn = column;
        
        while (position < input.length() && 
               (Character.isDigit(input.charAt(position)) || input.charAt(position) == '.')) {
            position++;
            column++;
        }
        
        String value = input.substring(start, position);
        return new Token(TokenType.NUMBER, value, line, startColumn);
    }

    private Token readString() {
        char quote = input.charAt(position);
        int startColumn = column;
        position++;
        column++;
        
        StringBuilder sb = new StringBuilder();
        while (position < input.length() && input.charAt(position) != quote) {
            if (input.charAt(position) == '\\' && position + 1 < input.length()) {
                position++;
                column++;
                char next = input.charAt(position);
                switch (next) {
                    case 'n': sb.append('\n'); break;
                    case 't': sb.append('\t'); break;
                    case 'r': sb.append('\r'); break;
                    case '\\': sb.append('\\'); break;
                    case '\'': sb.append('\''); break;
                    case '"': sb.append('"'); break;
                    default: sb.append(next); break;
                }
            } else {
                sb.append(input.charAt(position));
            }
            position++;
            column++;
        }
        
        if (position < input.length()) {
            position++;
            column++;
        }
        
        return new Token(TokenType.STRING, sb.toString(), line, startColumn);
    }

    private Token readIdentifier() {
        int start = position;
        int startColumn = column;
        
        while (position < input.length() && 
               (Character.isLetterOrDigit(input.charAt(position)) || 
                input.charAt(position) == '_')) {
            position++;
            column++;
        }
        
        String value = input.substring(start, position);
        TokenType keyword = KEYWORDS.get(value.toUpperCase());
        
        if (keyword != null) {
            return new Token(keyword, value, line, startColumn);
        }
        
        return new Token(TokenType.IDENTIFIER, value, line, startColumn);
    }

    private Token readOperatorOrDelimiter() {
        char ch = input.charAt(position);
        int startColumn = column;
        position++;
        column++;
        
        switch (ch) {
            case ',':
                return new Token(TokenType.COMMA, ",", line, startColumn);
            case ';':
                return new Token(TokenType.SEMICOLON, ";", line, startColumn);
            case '.':
                return new Token(TokenType.DOT, ".", line, startColumn);
            case '(':
                return new Token(TokenType.LPAREN, "(", line, startColumn);
            case ')':
                return new Token(TokenType.RPAREN, ")", line, startColumn);
            case '+':
                return new Token(TokenType.PLUS, "+", line, startColumn);
            case '-':
                return new Token(TokenType.MINUS, "-", line, startColumn);
            case '*':
                return new Token(TokenType.MULTIPLY, "*", line, startColumn);
            case '/':
                return new Token(TokenType.DIVIDE, "/", line, startColumn);
            case '=':
                return new Token(TokenType.EQ, "=", line, startColumn);
            case '<':
                if (position < input.length() && input.charAt(position) == '=') {
                    position++;
                    column++;
                    return new Token(TokenType.LE, "<=", line, startColumn);
                } else if (position < input.length() && input.charAt(position) == '>') {
                    position++;
                    column++;
                    return new Token(TokenType.NE, "<>", line, startColumn);
                }
                return new Token(TokenType.LT, "<", line, startColumn);
            case '>':
                if (position < input.length() && input.charAt(position) == '=') {
                    position++;
                    column++;
                    return new Token(TokenType.GE, ">=", line, startColumn);
                }
                return new Token(TokenType.GT, ">", line, startColumn);
            case '!':
                if (position < input.length() && input.charAt(position) == '=') {
                    position++;
                    column++;
                    return new Token(TokenType.NE, "!=", line, startColumn);
                }
                throw new RuntimeException("Unexpected character: !");
            default:
                throw new RuntimeException("Unexpected character: " + ch);
        }
    }

    private void skipWhitespace() {
        while (position < input.length()) {
            char ch = input.charAt(position);
            if (ch == ' ' || ch == '\t') {
                position++;
                column++;
            } else if (ch == '\n') {
                position++;
                line++;
                column = 1;
            } else if (ch == '\r') {
                position++;
                if (position < input.length() && input.charAt(position) == '\n') {
                    position++;
                }
                line++;
                column = 1;
            } else {
                break;
            }
        }
    }
}

