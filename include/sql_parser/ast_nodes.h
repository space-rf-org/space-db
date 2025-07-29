#pragma once

#include <memory>
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include "token.h"

namespace sql_parser {

struct ASTNode {
    virtual ~ASTNode() = default;
    [[nodiscard]] virtual std::string toString() const = 0;
};

struct Identifier final : ASTNode {
    std::string name;
    std::optional<std::string> alias;
    
    explicit Identifier(std::string n) : name(std::move(n)) {}
    [[nodiscard]] std::string toString() const override;
};

struct Literal final : ASTNode {
    std::variant<std::string, int, double, bool, std::nullptr_t> value;

    template<typename T>
    explicit Literal(T val) : value(val) {}

    [[nodiscard]] std::string toString() const override;
};

struct BinaryExpression final : ASTNode {
    std::unique_ptr<ASTNode> left;
    TokenType operator_;
    std::unique_ptr<ASTNode> right;

    BinaryExpression(std::unique_ptr<ASTNode> l, TokenType op, std::unique_ptr<ASTNode> r)
        : left(std::move(l)), operator_(op), right(std::move(r)) {}
    [[nodiscard]] std::string toString() const override;

private:
    [[nodiscard]] static std::string tokenToString(TokenType type);
};

struct UnaryExpression final : ASTNode {
    TokenType operator_;
    std::unique_ptr<ASTNode> operand;

    UnaryExpression(TokenType op, std::unique_ptr<ASTNode> operand)
        : operator_(op), operand(std::move(operand)) {}
    [[nodiscard]] std::string toString() const override {
        return Token::tokenTypeToString(operator_) + operand->toString();
    };
};

struct IntervalExpression final : ASTNode {
    std::unique_ptr<ASTNode> literal_value;    // StringConstant or NumericConstant
    std::optional<TokenType> unit;             // Optional unit (DAY, MONTH, etc.)

    explicit IntervalExpression(std::unique_ptr<ASTNode> val, const std::optional<TokenType> u = std::nullopt)
        : literal_value(std::move(val)), unit(u) {}

    [[nodiscard]] std::string toString() const override {
        if (!unit) return literal_value->toString();
        return literal_value->toString() + " " + unitToString(unit.value());
    };

private:
    [[nodiscard]] static std::string unitToString(const TokenType unit) { return Token::tokenTypeToString(unit);};
};

enum class ConstantType {
    STRING,
    NUMBER,
    BOOLEAN,
    NULLTYPE
};

struct ConstantExpression final : ASTNode {
    std::string value;
    ConstantType type;

    ConstantExpression(std::string val, ConstantType t)
        : value(std::move(val)), type(t) {}

    // Convenience constructor for auto-inferred type
    explicit ConstantExpression(std::string val)
        : value(std::move(val)), type(inferType(value)) {}

    [[nodiscard]] std::string toString() const override {
        switch (type) {
            case ConstantType::STRING: return "'" + value + "'";
            case ConstantType::NUMBER: return value;
            case ConstantType::BOOLEAN: return value == "true" ? "TRUE" : "FALSE";
            case ConstantType::NULLTYPE: return "NULL";
        }
        return "<INVALID_CONSTANT>";
    }

private:
    static ConstantType inferType(const std::string& val) {
        if (val == "true" || val == "false") return ConstantType::BOOLEAN;
        if (val == "null" || val == "NULL") return ConstantType::NULLTYPE;
        if (!val.empty() && (isdigit(val[0]) || val[0] == '-')) return ConstantType::NUMBER;
        return ConstantType::STRING;
    }
};



struct FunctionCall final : ASTNode {
    std::string name;
    std::vector<std::unique_ptr<ASTNode>> arguments;
    bool distinct = false;  // For COUNT(DISTINCT x)

    explicit FunctionCall(std::string n) : name(std::move(n)) {}
    [[nodiscard]] std::string toString() const override;
};

struct WindowFunction final : ASTNode {
    std::unique_ptr<FunctionCall> function;
    std::vector<std::unique_ptr<ASTNode>> partitionBy;
    std::vector<std::pair<std::unique_ptr<ASTNode>, bool>> orderBy; // bool: true=ASC, false=DESC
    std::optional<std::string> frameClause; // "ROWS BETWEEN...", "RANGE BETWEEN..."

    explicit WindowFunction(std::unique_ptr<FunctionCall> func) : function(std::move(func)) {}
    [[nodiscard]] std::string toString() const override;
};

struct CaseExpression final : ASTNode {
    struct WhenClause {
        std::unique_ptr<ASTNode> condition;
        std::unique_ptr<ASTNode> result;
    };

    std::optional<std::unique_ptr<ASTNode>> caseExpression; // For searched vs simple CASE
    std::vector<WhenClause> whenClauses;
    std::unique_ptr<ASTNode> elseClause;

    [[nodiscard]] std::string toString() const override;
};

struct CastExpression final : ASTNode {
    std::unique_ptr<ASTNode> expression;
    std::string targetType;

    CastExpression(std::unique_ptr<ASTNode> expr, std::string type)
        : expression(std::move(expr)), targetType(std::move(type)) {}
    [[nodiscard]] std::string toString() const override;
};

    struct InExpression final : ASTNode {
        std::unique_ptr<ASTNode> expression;                         // LHS of IN
        std::vector<std::unique_ptr<ASTNode>> valueList;             // For IN (...)
        std::unique_ptr<ASTNode> subquery;                           // For IN (SELECT ...)
        bool notIn = false;

        // For value list
        InExpression(std::unique_ptr<ASTNode> expr,
                     std::vector<std::unique_ptr<ASTNode>> values,
                     bool not_flag)
            : expression(std::move(expr)),
              valueList(std::move(values)),
              notIn(not_flag) {}

        // For subquery
        InExpression(std::unique_ptr<ASTNode> expr,
                     std::unique_ptr<ASTNode> subquery_expr,
                     bool not_flag)
            : expression(std::move(expr)),
              subquery(std::move(subquery_expr)),
              notIn(not_flag) {}

        [[nodiscard]] std::string toString() const override {
            std::string result = expression->toString();
            result += notIn ? " NOT IN (" : " IN (";
            if (subquery) {
                result += subquery->toString();
            } else {
                for (size_t i = 0; i < valueList.size(); ++i) {
                    result += valueList[i]->toString();
                    if (i + 1 < valueList.size()) result += ", ";
                }
            }
            result += ")";
            return result;
        }
    };


struct ExistsExpression final : ASTNode {
    std::unique_ptr<ASTNode> subquery;
    bool notExists = false; // For NOT EXISTS

    explicit ExistsExpression(std::unique_ptr<ASTNode> query) : subquery(std::move(query)) {}
    [[nodiscard]] std::string toString() const override;
};

struct BetweenExpression final : ASTNode {
    std::unique_ptr<ASTNode> expression;
    std::unique_ptr<ASTNode> lowerBound;
    std::unique_ptr<ASTNode> upperBound;
    bool notBetween = false; // For NOT BETWEEN

    BetweenExpression(std::unique_ptr<ASTNode> expr,
                     std::unique_ptr<ASTNode> lower,
                     std::unique_ptr<ASTNode> upper)
        : expression(std::move(expr)), lowerBound(std::move(lower)), upperBound(std::move(upper)) {}
    [[nodiscard]] std::string toString() const override;
};

struct LikeExpression final : ASTNode {
    std::unique_ptr<ASTNode> expression;
    std::unique_ptr<ASTNode> pattern;
    std::unique_ptr<ASTNode> escapeChar; // Optional ESCAPE clause
    bool notLike = false; // For NOT LIKE
    bool ilike = false; // For PostgreSQL ILIKE

    LikeExpression(std::unique_ptr<ASTNode> expr, std::unique_ptr<ASTNode> pat)
        : expression(std::move(expr)), pattern(std::move(pat)) {}
    [[nodiscard]] std::string toString() const override;
};

struct SubQuery final : ASTNode {
    std::unique_ptr<ASTNode> query;

    explicit SubQuery(std::unique_ptr<ASTNode> q) : query(std::move(q)) {}
    [[nodiscard]] std::string toString() const override;
};

struct TableReference final : ASTNode {
    std::string name;
    std::optional<std::string> schema; // For schema.table
    std::optional<std::string> alias;
    std::vector<std::unique_ptr<ASTNode>> joins;

    explicit TableReference(std::string n) : name(std::move(n)) {}
    [[nodiscard]] std::string toString() const override;
};

struct JoinClause final : ASTNode {
    enum class JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL_OUTER,
        CROSS,
        NATURAL_INNER,
        NATURAL_LEFT,
        NATURAL_RIGHT
    };

    JoinType joinType;
    std::unique_ptr<ASTNode> table;
    std::unique_ptr<ASTNode> condition; // ON condition
    std::vector<std::string> usingColumns; // USING(col1, col2, ...)

    JoinClause(const JoinType type, std::unique_ptr<ASTNode> t)
        : joinType(type), table(std::move(t)) {}
    JoinClause(JoinType type, std::unique_ptr<ASTNode> t, std::unique_ptr<ASTNode> cond)
        : joinType(type), table(std::move(t)), condition(std::move(cond)) {}
    [[nodiscard]] std::string toString() const override;
};

struct CTEDefinition final : ASTNode {
    std::string name;
    std::vector<std::string> columnNames; // Optional column list
    std::unique_ptr<ASTNode> query;

    CTEDefinition(std::string n, std::unique_ptr<ASTNode> q)
        : name(std::move(n)), query(std::move(q)) {}
    [[nodiscard]] std::string toString() const override;
};

struct WithClause final : ASTNode {
    std::vector<std::unique_ptr<CTEDefinition>> ctes;
    bool recursive = false; // For WITH RECURSIVE

    [[nodiscard]] std::string toString() const override;
};

struct SelectStatement final : ASTNode {
    std::unique_ptr<WithClause> withClause; // Common Table Expressions
    bool distinct = false;
    std::vector<std::unique_ptr<ASTNode>> selectList;
    std::vector<std::unique_ptr<ASTNode>> fromClause;
    std::unique_ptr<ASTNode> whereClause;
    std::vector<std::unique_ptr<ASTNode>> groupByClause;
    std::unique_ptr<ASTNode> havingClause;
    std::vector<std::pair<std::unique_ptr<ASTNode>, bool>> orderByClause; // bool: true=ASC, false=DESC
    std::optional<int> limitClause;
    std::optional<int> offsetClause;

    [[nodiscard]] std::string toString() const override;
};

struct UnionStatement final : ASTNode {
    enum class UnionType { UNION, UNION_ALL, INTERSECT, EXCEPT };

    std::unique_ptr<ASTNode> left;
    UnionType unionType;
    std::unique_ptr<ASTNode> right;

    UnionStatement(std::unique_ptr<ASTNode> l, UnionType type, std::unique_ptr<ASTNode> r)
        : left(std::move(l)), unionType(type), right(std::move(r)) {}
    [[nodiscard]] std::string toString() const override;
};

// DML Statements
struct InsertStatement final : ASTNode {
    std::string tableName;
    std::optional<std::string> schema;
    std::vector<std::string> columns; // Optional column list
    std::unique_ptr<ASTNode> values; // VALUES clause or SELECT statement
    std::unique_ptr<ASTNode> onConflict; // PostgreSQL ON CONFLICT, SQLite ON CONFLICT

    explicit InsertStatement(std::string table) : tableName(std::move(table)) {}
    [[nodiscard]] std::string toString() const override;
};

struct UpdateStatement final : ASTNode {
    std::string tableName;
    std::optional<std::string> schema;
    std::optional<std::string> alias;
    std::vector<std::pair<std::string, std::unique_ptr<ASTNode>>> setClause; // column = expression
    std::unique_ptr<ASTNode> fromClause; // PostgreSQL UPDATE ... FROM
    std::unique_ptr<ASTNode> whereClause;

    explicit UpdateStatement(std::string table) : tableName(std::move(table)) {}
    [[nodiscard]] std::string toString() const override;
};

struct DeleteStatement final : ASTNode {
    std::string tableName;
    std::optional<std::string> schema;
    std::optional<std::string> alias;
    std::unique_ptr<ASTNode> whereClause;

    explicit DeleteStatement(std::string table) : tableName(std::move(table)) {}
    [[nodiscard]] std::string toString() const override;
};

// DDL Statements
struct CreateTableStatement final : ASTNode {
    struct ColumnDefinition {
        std::string name;
        std::string dataType;
        std::vector<std::string> constraints; // NOT NULL, PRIMARY KEY, etc.
        std::unique_ptr<ASTNode> defaultValue;
    };

    std::string tableName;
    std::optional<std::string> schema;
    std::vector<ColumnDefinition> columns;
    std::vector<std::string> tableConstraints; // PRIMARY KEY(col1, col2), FOREIGN KEY, etc.
    bool ifNotExists = false;
    bool temporary = false;

    explicit CreateTableStatement(std::string table) : tableName(std::move(table)) {}
    [[nodiscard]] std::string toString() const override;
};

struct DropTableStatement final : ASTNode {
    std::vector<std::string> tableNames;
    std::optional<std::string> schema;
    bool ifExists = false;
    bool cascade = false;

    explicit DropTableStatement(std::vector<std::string> tables) : tableNames(std::move(tables)) {}
    [[nodiscard]] std::string toString() const override;
};

struct CreateIndexStatement final : ASTNode {
    std::string indexName;
    std::string tableName;
    std::optional<std::string> schema;
    std::vector<std::pair<std::string, bool>> columns; // column name, is_desc
    bool unique = false;
    bool ifNotExists = false;
    std::unique_ptr<ASTNode> whereClause; // Partial index condition

    CreateIndexStatement(std::string index, std::string table)
        : indexName(std::move(index)), tableName(std::move(table)) {}
    [[nodiscard]] std::string toString() const override;
};

// Utility Statements
struct ExplainStatement final : ASTNode {
    std::unique_ptr<ASTNode> statement;
    bool analyze = false;
    bool verbose = false;
    std::vector<std::pair<std::string, std::string>> options; // PostgreSQL EXPLAIN options

    explicit ExplainStatement(std::unique_ptr<ASTNode> stmt) : statement(std::move(stmt)) {}
    [[nodiscard]] std::string toString() const override;
};

// PostgreSQL-specific
struct ArrayExpression final : ASTNode {
    std::vector<std::unique_ptr<ASTNode>> elements;

    [[nodiscard]] std::string toString() const override;
};

struct ArrayAccess final : ASTNode {
    std::unique_ptr<ASTNode> array;
    std::unique_ptr<ASTNode> index;

    ArrayAccess(std::unique_ptr<ASTNode> arr, std::unique_ptr<ASTNode> idx)
        : array(std::move(arr)), index(std::move(idx)) {}
    [[nodiscard]] std::string toString() const override;
};

// JSON operations (PostgreSQL, SQLite JSON1 extension)
struct JsonExpression final : ASTNode {
    enum class JsonOp {
        EXTRACT,      // ->, ->>
        PATH_EXTRACT, // #>, #>>
        CONTAINS,     // @>, <@
        EXISTS,       // ?
        ANY_EXISTS,   // ?|
        ALL_EXISTS    // ?&
    };

    std::unique_ptr<ASTNode> left;
    JsonOp operator_;
    std::unique_ptr<ASTNode> right;

    JsonExpression(std::unique_ptr<ASTNode> l, const JsonOp op, std::unique_ptr<ASTNode> r)
        : left(std::move(l)), operator_(op), right(std::move(r)) {}
    [[nodiscard]] std::string toString() const override;
};

} // namespace sql_parser