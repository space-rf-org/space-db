#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "token.h"

namespace sql_parser {

    class SQLLexer {
    private:
        std::string input;
        size_t position;
        size_t length;

        static const std::unordered_map<std::string, TokenType> keywords;

    public:
        explicit SQLLexer(std::string sql);
        std::vector<Token> tokenize();

    private:
        void skipWhitespace();
        Token nextToken();
        Token parseStringLiteral(char quote);
        Token parseNumber();
        Token parseIdentifierOrKeyword();
    };

} // namespace sql_parser