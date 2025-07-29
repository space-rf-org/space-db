#include "sql_parser/ast_nodes.h"
#include <sstream>

namespace sql_parser {


std::string Identifier::toString() const { 
    return alias ? name + " AS " + *alias : name; 
}

std::string Literal::toString() const {
    return std::visit([](const auto& v) -> std::string {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, std::string>) {
            return "'" + v + "'";
        } else if constexpr (std::is_same_v<T, bool>) {
            return v ? "TRUE" : "FALSE";
        } else if constexpr (std::is_same_v<T, std::nullptr_t>) {
            return "NULL";
        } else if constexpr (std::is_arithmetic_v<T>) {
            return std::to_string(v);
        } else {
            static_assert(sizeof(T) == 0, "Unsupported literal type");
        }
        return "";
    }, value);
}

std::string BinaryExpression::toString() const {
    return "(" + left->toString() + " " + tokenToString(operator_) + " " + right->toString() + ")";
}

std::string BinaryExpression::tokenToString(TokenType type) {
    switch(type) {
        case TokenType::EQUALS: return "=";
        case TokenType::NOT_EQUALS: return "!=";
        case TokenType::LESS_THAN: return "<";
        case TokenType::GREATER_THAN: return ">";
        case TokenType::LESS_EQUAL: return "<=";
        case TokenType::GREATER_EQUAL: return ">=";
        case TokenType::PLUS: return "+";
        case TokenType::MINUS: return "-";
        case TokenType::MULTIPLY: return "*";
        case TokenType::DIVIDE: return "/";
        case TokenType::AND: return "AND";
        case TokenType::OR: return "OR";
        default: return "UNKNOWN_OP";
    }
}

std::string FunctionCall::toString() const {
    std::string result = name + "(";
    for (size_t i = 0; i < arguments.size(); ++i) {
        if (i > 0) result += ", ";
        result += arguments[i]->toString();
    }
    result += ")";
    return result;
}

std::string CaseExpression::toString() const {
    std::string result = "CASE ";
    for (const auto& when : whenClauses) {
        result += "WHEN " + when.condition->toString() + " THEN " + when.result->toString() + " ";
    }
    if (elseClause) {
        result += "ELSE " + elseClause->toString() + " ";
    }
    result += "END";
    return result;
}

std::string SubQuery::toString() const {
    return "(" + query->toString() + ")";
}

std::string SelectStatement::toString() const {
    std::string result = "SELECT ";
    for (size_t i = 0; i < selectList.size(); ++i) {
        if (i > 0) result += ", ";
        result += selectList[i]->toString();
    }
    
    if (!fromClause.empty()) {
        result += " FROM ";
        for (size_t i = 0; i < fromClause.size(); ++i) {
            if (i > 0) result += ", ";
            result += fromClause[i]->toString();
        }
    }
    
    if (whereClause) {
        result += " WHERE " + whereClause->toString();
    }
    
    if (!groupByClause.empty()) {
        result += " GROUP BY ";
        for (size_t i = 0; i < groupByClause.size(); ++i) {
            if (i > 0) result += ", ";
            result += groupByClause[i]->toString();
        }
    }
    
    if (havingClause) {
        result += " HAVING " + havingClause->toString();
    }
    
    if (!orderByClause.empty()) {
        result += " ORDER BY ";
        for (size_t i = 0; i < orderByClause.size(); ++i) {
            if (i > 0) result += ", ";
            result += orderByClause[i].first->toString();
            result += orderByClause[i].second ? " ASC" : " DESC";
        }
    }
    
    if (limitClause) {
        result += " LIMIT " + std::to_string(*limitClause);
    }
    
    if (offsetClause) {
        result += " OFFSET " + std::to_string(*offsetClause);
    }
    
    return result;
}

std::string JoinClause::toString() const {
    std::string result;

    // Handle join type
    switch (joinType) {
        case JoinType::INNER:
            result = "INNER JOIN ";
            break;
        case JoinType::LEFT:
            result = "LEFT JOIN ";
            break;
        case JoinType::RIGHT:
            result = "RIGHT JOIN ";
            break;
        case JoinType::FULL_OUTER:
            result = "FULL OUTER JOIN ";
            break;
        case JoinType::CROSS:
            result = "CROSS JOIN ";
            break;
        case JoinType::NATURAL_INNER:
            result = "NATURAL JOIN ";
            break;
        case JoinType::NATURAL_LEFT:
            result = "NATURAL LEFT JOIN ";
            break;
        case JoinType::NATURAL_RIGHT:
            result = "NATURAL RIGHT JOIN ";
            break;
    }

    // Add table reference
    result += table->toString();

    // Add join condition - ON clause takes precedence over USING
    if (condition) {
        result += " ON " + condition->toString();
    }
    // Add USING clause if no ON condition and USING columns exist
    else if (!usingColumns.empty()) {
        result += " USING (";
        for (size_t i = 0; i < usingColumns.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += usingColumns[i];
        }
        result += ")";
    }
    // Note: CROSS JOIN and NATURAL JOINs don't use ON/USING conditions
    // CROSS JOIN has no condition
    // NATURAL JOIN automatically matches columns with same names

    return result;
}

} // namespace sql_parser