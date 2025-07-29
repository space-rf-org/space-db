#pragma once

#include <vector>
#include <memory>
#include "token.h"
#include "ast_nodes.h"

namespace sql_parser {

    class SQLParser {
    private:
        std::vector<Token> tokens;
        size_t currentToken;

    public:
        explicit SQLParser(std::vector<Token> tokens);
        std::unique_ptr<ASTNode> parse();

    private:
        const Token& peek() const;
        const Token& advance();

        const Token &lookahead();

        bool match(TokenType type);
        void expect(TokenType type);

        std::unique_ptr<SelectStatement> parseSelectStatement();
        std::unique_ptr<ASTNode> parseTableReference();
        std::unique_ptr<JoinClause> parseJoinClause();
        std::unique_ptr<ASTNode> parseExpression();
        std::unique_ptr<ASTNode> parseOrExpression();
        std::unique_ptr<ASTNode> parseAndExpression();

        std::unique_ptr<ASTNode> parseNotExpression();

        std::unique_ptr<ASTNode> parseInExpression();

        std::unique_ptr<ASTNode> parseExistsExpression();

        std::unique_ptr<ASTNode> parseEqualityExpression();
        std::unique_ptr<ASTNode> parseRelationalExpression();

        std::unique_ptr<ASTNode> parseLikeExpression();

        std::unique_ptr<ASTNode> parseAdditiveExpression();
        std::unique_ptr<ASTNode> parseMultiplicativeExpression();

        std::unique_ptr<ASTNode> parseUnaryExpression();

        std::unique_ptr<ASTNode> parseIntervalExpression();

        std::unique_ptr<ASTNode> parsePrimaryExpression();
        std::unique_ptr<CaseExpression> parseCaseExpression();
        std::unique_ptr<FunctionCall> parseFunctionCall();

        static std::unique_ptr<ASTNode> cloneExpression(ASTNode *node);
    };

#include <stdexcept>

    class ParserException final : public std::runtime_error {
    public:
        explicit ParserException(const std::string& msg)
            : std::runtime_error("Parser error: " + msg) {}
    };


} // namespace sql_parser