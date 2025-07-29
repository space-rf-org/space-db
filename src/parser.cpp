#include "sql_parser/parser.h"
#include <iostream>
#include <stdexcept>
#include <memory>

namespace sql_parser {
    SQLParser::SQLParser(std::vector<Token> tokens)
        : tokens(std::move(tokens)), currentToken(0) {
    }

    std::unique_ptr<ASTNode> SQLParser::parse() {
        return parseSelectStatement();
    }

    const Token &SQLParser::peek() const {
        if (currentToken >= tokens.size()) {
            static Token eofToken{TokenType::EOF_TOKEN, "", 0};
            return eofToken;
        }
        return tokens[currentToken];
    }

    const Token &SQLParser::advance() {
        if (currentToken < tokens.size()) {
            currentToken++;
        }
        return tokens[currentToken - 1];
    }

    const Token &SQLParser::lookahead() {
        if (const size_t lookaheadToken = currentToken + 1; lookaheadToken < tokens.size()) {
            return tokens[lookaheadToken];
        }
        static const Token eof_token{TokenType::EOF_TOKEN, "", 0};
        return eof_token;
    }

    bool SQLParser::match(const TokenType type) {
        if (peek().type == type) {
            advance();
            return true;
        }
        return false;
    }

    void SQLParser::expect(const TokenType type) {
        if (!match(type)) {
            throw std::runtime_error("Expected token type " + Token::tokenTypeToString(type) +
                                     " but got " + Token::tokenTypeToString(peek().type) +
                                     " at position " + std::to_string(peek().position));
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<SelectStatement> SQLParser::parseSelectStatement() {
        auto stmt = std::make_unique<SelectStatement>();
        expect(TokenType::SELECT);
        // Parse DISTINCT if present
        if (match(TokenType::DISTINCT)) {
            stmt->distinct = true;
        }
        // Parse SELECT list
        do {
            auto expr = parseExpression();
            // Handle alias for expressions in SELECT list
            std::string alias;
            if (match(TokenType::AS)) {
                if (peek().type == TokenType::IDENTIFIER) {
                    alias = advance().value;
                }
            } else if (peek().type == TokenType::IDENTIFIER &&
                       !Token::isReservedKeyword(peek().value)) {
                alias = advance().value;
            }

            if (!alias.empty()) {
                // If it's an identifier, set the alias directly
                if (auto identifier = dynamic_cast<Identifier *>(expr.get())) {
                    identifier->alias = alias;
                } else {
                    // For other expressions, wrap in a structure that supports aliases
                    // This is a simplification - in practice you'd want a SelectExpression wrapper
                    stmt->selectList.push_back(std::move(expr));
                }
            } else {
                stmt->selectList.push_back(std::move(expr));
            }
        } while (match(TokenType::COMMA));

        // Parse FROM clause
        if (match(TokenType::FROM)) {
            do {
                stmt->fromClause.push_back(parseTableReference());
            } while (match(TokenType::COMMA));

            // Parse JOINs
            while (peek().type == TokenType::LEFT || peek().type == TokenType::RIGHT ||
                   peek().type == TokenType::INNER || peek().type == TokenType::JOIN ||
                   peek().type == TokenType::FULL) {
                stmt->fromClause.push_back(parseJoinClause());
            }
        }

        // Parse WHERE clause
        if (match(TokenType::WHERE)) {
            stmt->whereClause = parseExpression();
        }

        // Parse GROUP BY clause
        if (match(TokenType::GROUP) && match(TokenType::BY)) {
            do {
                stmt->groupByClause.push_back(parseExpression());
            } while (match(TokenType::COMMA));
        }

        // Parse HAVING clause
        if (match(TokenType::HAVING)) {
            stmt->havingClause = parseExpression();
        }

        // Parse ORDER BY clause
        if (match(TokenType::ORDER) && match(TokenType::BY)) {
            do {
                auto expr = parseExpression();
                bool ascending = true;
                if (match(TokenType::DESC)) {
                    ascending = false;
                } else {
                    match(TokenType::ASC); // Optional ASC
                }
                stmt->orderByClause.emplace_back(std::move(expr), ascending);
            } while (match(TokenType::COMMA));
        }

        // Parse LIMIT clause
        if (match(TokenType::LIMIT)) {
            if (peek().type == TokenType::NUMBER_LITERAL) {
                stmt->limitClause = std::stoi(advance().value);
            }
        }

        // Parse OFFSET clause
        if (match(TokenType::OFFSET)) {
            if (peek().type == TokenType::NUMBER_LITERAL) {
                stmt->offsetClause = std::stoi(advance().value);
            }
        }

        return stmt;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseTableReference() {
        auto table = parseExpression();

        // Handle alias
        if (match(TokenType::AS) || (peek().type == TokenType::IDENTIFIER && !Token::isReservedKeyword(peek().value))) {
            if (peek().type == TokenType::IDENTIFIER) {
                if (const auto identifier = dynamic_cast<Identifier *>(table.get())) {
                    identifier->alias = advance().value;
                }
            }
        }

        return table;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<JoinClause> SQLParser::parseJoinClause() {
        JoinClause::JoinType joinType = JoinClause::JoinType::INNER;

        if (match(TokenType::LEFT)) {
            joinType = JoinClause::JoinType::LEFT;
            if (match(TokenType::OUTER)) {
                // LEFT OUTER JOIN
            }
            expect(TokenType::JOIN);
        } else if (match(TokenType::RIGHT)) {
            joinType = JoinClause::JoinType::RIGHT;
            if (match(TokenType::OUTER)) {
                // RIGHT OUTER JOIN
            }
            expect(TokenType::JOIN);
        } else if (match(TokenType::FULL)) {
            joinType = JoinClause::JoinType::FULL_OUTER;
            if (match(TokenType::OUTER)) {
                // FULL OUTER JOIN
            }
            expect(TokenType::JOIN);
        } else if (match(TokenType::INNER)) {
            joinType = JoinClause::JoinType::INNER;
            expect(TokenType::JOIN);
        } else if (match(TokenType::JOIN)) {
            joinType = JoinClause::JoinType::INNER;
        }

        auto table = parseTableReference();
        std::unique_ptr<ASTNode> condition = nullptr;

        if (match(TokenType::ON)) {
            condition = parseExpression();
        } else if (match(TokenType::USING)) {
            expect(TokenType::LEFT_PAREN);
            // Parse column list for USING clause
            // For simplicity, treating as expression
            condition = parseExpression();
            expect(TokenType::RIGHT_PAREN);
        }

        return std::make_unique<JoinClause>(joinType, std::move(table), std::move(condition));
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseExpression() {
        return parseOrExpression();
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseOrExpression() {
        auto left = parseAndExpression();

        while (match(TokenType::OR)) {
            auto right = parseAndExpression();
            left = std::make_unique<BinaryExpression>(std::move(left), TokenType::OR, std::move(right));
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseAndExpression() {
        auto left = parseNotExpression();

        while (match(TokenType::AND)) {
            auto right = parseNotExpression();
            left = std::make_unique<BinaryExpression>(std::move(left), TokenType::AND, std::move(right));
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseNotExpression() {
        if (match(TokenType::NOT)) {
            auto operand = parseNotExpression();
            return std::make_unique<UnaryExpression>(TokenType::NOT, std::move(operand));
        }

        return parseInExpression();
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseInExpression() {
        auto left = parseExistsExpression();

        if (match(TokenType::NOT)) {
            if (match(TokenType::IN)) {
                expect(TokenType::LEFT_PAREN);
                std::vector<std::unique_ptr<ASTNode> > values;

                if (peek().type == TokenType::SELECT) {
                    // Subquery
                    auto subquery = parseSelectStatement();
                    expect(TokenType::RIGHT_PAREN);
                    auto inExpr = std::make_unique<InExpression>(std::move(left),
                                                                 std::make_unique<SubQuery>(std::move(subquery)), true);
                    return inExpr;
                }
                // Value list
                do {
                    values.push_back(parseExpression());
                } while (match(TokenType::COMMA));
                expect(TokenType::RIGHT_PAREN);
                auto inExpr = std::make_unique<InExpression>(std::move(left), std::move(values), true);
                return inExpr;
            }
            // Put back the NOT token if it wasn't followed by IN
            currentToken--;
        } else if (match(TokenType::IN)) {
            expect(TokenType::LEFT_PAREN);
            std::vector<std::unique_ptr<ASTNode> > values;

            if (peek().type == TokenType::SELECT) {
                // Subquery
                auto subquery = parseSelectStatement();
                expect(TokenType::RIGHT_PAREN);
                auto inExpr = std::make_unique<InExpression>(std::move(left),
                                                             std::make_unique<SubQuery>(std::move(subquery)), false);
                return inExpr;
            } else {
                // Value list
                do {
                    values.push_back(parseExpression());
                } while (match(TokenType::COMMA));
                expect(TokenType::RIGHT_PAREN);
                auto inExpr = std::make_unique<InExpression>(std::move(left), std::move(values), false);
                return inExpr;
            }
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseExistsExpression() {
        if (match(TokenType::EXISTS)) {
            expect(TokenType::LEFT_PAREN);
            auto subquery = parseSelectStatement();
            expect(TokenType::RIGHT_PAREN);
            auto existsFunc = std::make_unique<FunctionCall>("EXISTS");
            existsFunc->arguments.push_back(std::make_unique<SubQuery>(std::move(subquery)));
            return existsFunc;
        }

        return parseEqualityExpression();
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseEqualityExpression() {
        auto left = parseRelationalExpression();

        while (peek().type == TokenType::EQUALS || peek().type == TokenType::NOT_EQUALS ||
               peek().type == TokenType::IS) {
            if (match(TokenType::IS)) {
                const bool isNot = match(TokenType::NOT);
                if (match(TokenType::NULL_LITERAL)) {
                    TokenType op = isNot ? TokenType::IS_NOT_NULL : TokenType::IS_NULL;
                    auto nullLiteral = std::make_unique<Literal>(nullptr); // NULL value
                    left = std::make_unique<BinaryExpression>(std::move(left), op, std::move(nullLiteral));
                }
            } else {
                TokenType op = advance().type;
                auto right = parseRelationalExpression();
                left = std::make_unique<BinaryExpression>(std::move(left), op, std::move(right));
            }
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseRelationalExpression() {
        auto left = parseLikeExpression();

        while (peek().type == TokenType::LESS_THAN || peek().type == TokenType::GREATER_THAN ||
               peek().type == TokenType::LESS_EQUAL || peek().type == TokenType::GREATER_EQUAL ||
               peek().type == TokenType::BETWEEN) {
            if (match(TokenType::BETWEEN)) {
                auto lower = parseLikeExpression();
                expect(TokenType::AND);
                auto upper = parseLikeExpression();

                // Create proper BETWEEN expression by cloning left operand
                auto leftClone = cloneExpression(left.get());
                auto lowerBound = std::make_unique<BinaryExpression>(
                    std::move(left), TokenType::GREATER_EQUAL, std::move(lower));
                auto upperBound = std::make_unique<BinaryExpression>(
                    std::move(leftClone), TokenType::LESS_EQUAL, std::move(upper));
                left = std::make_unique<BinaryExpression>(std::move(lowerBound), TokenType::AND, std::move(upperBound));
            } else {
                TokenType op = advance().type;
                auto right = parseLikeExpression();
                left = std::make_unique<BinaryExpression>(std::move(left), op, std::move(right));
            }
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseLikeExpression() {
        auto left = parseAdditiveExpression();

        if (match(TokenType::NOT)) {
            if (match(TokenType::LIKE)) {
                auto right = parseAdditiveExpression();
                left = std::make_unique<BinaryExpression>(std::move(left), TokenType::NOT_LIKE, std::move(right));
            } else {
                // Put back the NOT token
                currentToken--;
            }
        } else if (match(TokenType::LIKE)) {
            auto right = parseAdditiveExpression();
            left = std::make_unique<BinaryExpression>(std::move(left), TokenType::LIKE, std::move(right));
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseAdditiveExpression() {
        auto left = parseMultiplicativeExpression();

        while (peek().type == TokenType::PLUS || peek().type == TokenType::MINUS) {
            TokenType op = advance().type;
            auto right = parseMultiplicativeExpression();
            left = std::make_unique<BinaryExpression>(std::move(left), op, std::move(right));
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseMultiplicativeExpression() {
        auto left = parseUnaryExpression();

        while (peek().type == TokenType::MULTIPLY || peek().type == TokenType::DIVIDE ||
               peek().type == TokenType::MODULO) {
            TokenType op = advance().type;
            auto right = parseUnaryExpression();
            left = std::make_unique<BinaryExpression>(std::move(left), op, std::move(right));
        }

        return left;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseUnaryExpression() {
        if (peek().type == TokenType::PLUS || peek().type == TokenType::MINUS) {
            TokenType op = advance().type;
            auto operand = parseUnaryExpression();
            return std::make_unique<UnaryExpression>(op, std::move(operand));
        }

        return parseIntervalExpression();
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parseIntervalExpression() {
        if (!match(TokenType::INTERVAL)) {
            return parsePrimaryExpression(); // fallback
        }

        std::unique_ptr<ASTNode> value_expr;

        // Parse the interval literal (string or number)
        if (peek().type == TokenType::STRING_LITERAL) {
            value_expr = std::make_unique<ConstantExpression>(advance().value); // String like '30 days'
        } else if (peek().type == TokenType::NUMBER_LITERAL) {
            value_expr = std::make_unique<ConstantExpression>(advance().value); // Just a number like 30
        } else {
            throw ParserException("Expected string or number literal after INTERVAL");
        }

        // Parse optional unit
        std::optional<TokenType> unit;
        if (peek().type == TokenType::IDENTIFIER) {
            std::string unitStr = advance().value;
            std::transform(unitStr.begin(), unitStr.end(), unitStr.begin(), ::toupper);

            if (unitStr == "DAY" || unitStr == "DAYS") unit = TokenType::DAY;
            else if (unitStr == "HOUR" || unitStr == "HOURS") unit = TokenType::HOUR;
            else if (unitStr == "MINUTE" || unitStr == "MINUTES") unit = TokenType::MINUTE;
            else if (unitStr == "SECOND" || unitStr == "SECONDS") unit = TokenType::SECOND;
            else if (unitStr == "MONTH" || unitStr == "MONTHS") unit = TokenType::MONTH;
            else if (unitStr == "YEAR" || unitStr == "YEARS") unit = TokenType::YEAR;
            else throw ParserException("Unrecognized interval unit: " + unitStr);
        }

        return std::make_unique<IntervalExpression>(std::move(value_expr), unit);
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<ASTNode> SQLParser::parsePrimaryExpression() {
        // Handle parentheses
        if (match(TokenType::LEFT_PAREN)) {
            // Check if it's a subquery
            if (peek().type == TokenType::SELECT) {
                auto subquery = parseSelectStatement();
                expect(TokenType::RIGHT_PAREN);
                return std::make_unique<SubQuery>(std::move(subquery));
            }
            auto expr = parseExpression();
            expect(TokenType::RIGHT_PAREN);
            return expr;
        }

        // Handle CASE expressions
        if (match(TokenType::CASE)) {
            return parseCaseExpression();
        }

        // Handle CAST expressions
        if (match(TokenType::CAST)) {
            expect(TokenType::LEFT_PAREN);
            auto expr = parseExpression();
            expect(TokenType::AS);
            if (peek().type == TokenType::IDENTIFIER) {
                std::string dataType = advance().value;
                expect(TokenType::RIGHT_PAREN);
                auto castFunc = std::make_unique<FunctionCall>("CAST");
                castFunc->arguments.push_back(std::move(expr));
                castFunc->arguments.push_back(std::make_unique<Literal>(dataType));
                return castFunc;
            }
        }

        // Handle function calls - check this before identifiers
        if (Token::isFunctionToken(peek().type) ||
            (peek().type == TokenType::IDENTIFIER && Token::isFunction(peek().value))) {
            return parseFunctionCall();
        }

        // Handle identifiers with optional table prefix
        if (peek().type == TokenType::IDENTIFIER) {
            std::string name = advance().value;

            // Handle table.column syntax
            if (match(TokenType::DOT)) {
                if (peek().type == TokenType::IDENTIFIER) {
                    name += "." + advance().value;
                } else if (peek().type == TokenType::MULTIPLY) {
                    name += ".*";
                    advance();
                }
            }

            auto identifier = std::make_unique<Identifier>(name);
            return identifier;
        }

        // Handle string literals
        if (peek().type == TokenType::STRING_LITERAL) {
            return std::make_unique<Literal>(advance().value);
        }

        // Handle * (asterisk)
        if (peek().type == TokenType::MULTIPLY) {
            return std::make_unique<Literal>(advance().value);
        }

        // Handle number literals
        if (peek().type == TokenType::NUMBER_LITERAL) {
            std::string value = advance().value;
            if (value.find('.') != std::string::npos) {
                return std::make_unique<Literal>(std::stod(value));
            }
            return std::make_unique<Literal>(std::stoi(value));
        }

        // Handle NULL literal
        if (peek().type == TokenType::NULL_LITERAL) {
            advance();
            return std::make_unique<Literal>(nullptr);
        }

        // Handle boolean literals
        if (peek().type == TokenType::TRUE_LITERAL) {
            advance();
            return std::make_unique<Literal>(true);
        }

        if (peek().type == TokenType::FALSE_LITERAL) {
            advance();
            return std::make_unique<Literal>(false);
        }

        // Handle date/time functions
        if (peek().type == TokenType::CURRENT_DATE) {
            advance();
            auto func = std::make_unique<FunctionCall>("CURRENT_DATE");
            return func;
        }

        if (peek().type == TokenType::NOW) {
            advance();
            auto func = std::make_unique<FunctionCall>("NOW");
            expect(TokenType::LEFT_PAREN);
            expect(TokenType::RIGHT_PAREN);
            return func;
        }

        throw std::runtime_error(
            "Unexpected token '" + peek().value + "' in expression at position " + std::to_string(peek().position));
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<CaseExpression> SQLParser::parseCaseExpression() {
        auto caseExpr = std::make_unique<CaseExpression>();

        while (match(TokenType::WHEN)) {
            CaseExpression::WhenClause when;
            when.condition = parseExpression();
            expect(TokenType::THEN);
            when.result = parseExpression();
            caseExpr->whenClauses.push_back(std::move(when));
        }

        if (match(TokenType::ELSE)) {
            caseExpr->elseClause = parseExpression();
        }

        expect(TokenType::END);
        return caseExpr;
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    std::unique_ptr<FunctionCall> SQLParser::parseFunctionCall() {
        const auto func_type = peek().type;
        std::string funcName = advance().value;
        auto func = std::make_unique<FunctionCall>(funcName);
        if (Token::isFunctionToken(func_type)) {
            if (peek().type != TokenType::LEFT_PAREN) {
                return func; // Handle aggregate functions separately
            }
        }
        expect(TokenType::LEFT_PAREN);

        // Handle special case for COUNT(*)
        if (funcName == "COUNT" && peek().type == TokenType::MULTIPLY) {
            func->arguments.push_back(std::make_unique<Literal>("*"));
            advance();
        } else if (peek().type != TokenType::RIGHT_PAREN) {
            // Handle DISTINCT for aggregate functions
            if (match(TokenType::DISTINCT)) {
                func->distinct = true;
            }

            do {
                func->arguments.push_back(parseExpression());
            } while (match(TokenType::COMMA));
        }

        expect(TokenType::RIGHT_PAREN);
        return func;
    }

    std::unique_ptr<ASTNode> SQLParser::cloneExpression(ASTNode *node) {
        if (!node) return nullptr;

        // This is a simplified clone - in practice you'd implement proper cloning
        // for each AST node type. For now, we'll create a new identifier if it's an identifier
        if (auto identifier = dynamic_cast<Identifier *>(node)) {
            return std::make_unique<Identifier>(identifier->name);
        } else if (auto literal = dynamic_cast<Literal *>(node)) {
            // Clone literal based on its type
            return std::make_unique<Literal>(literal->value);
        } else {
            // For complex expressions, this is a placeholder
            // In practice, you'd implement proper deep cloning
            throw std::runtime_error("Expression cloning not fully implemented for complex expressions");
        }
    }
} // namespace sql_parser
