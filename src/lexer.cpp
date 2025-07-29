#include "sql_parser/lexer.h"
#include <cctype>
#include <stdexcept>

namespace sql_parser {

const std::unordered_map<std::string, TokenType> SQLLexer::keywords = {
    {"SELECT", TokenType::SELECT}, {"FROM", TokenType::FROM}, {"WHERE", TokenType::WHERE},
    {"ORDER", TokenType::ORDER}, {"BY", TokenType::BY}, {"GROUP", TokenType::GROUP},
    {"HAVING", TokenType::HAVING}, {"LIMIT", TokenType::LIMIT}, {"OFFSET", TokenType::OFFSET},
    {"JOIN", TokenType::JOIN}, {"LEFT", TokenType::LEFT}, {"RIGHT", TokenType::RIGHT},
    {"INNER", TokenType::INNER}, {"OUTER", TokenType::OUTER}, {"ON", TokenType::ON},
    {"AS", TokenType::AS}, {"AND", TokenType::AND}, {"OR", TokenType::OR}, {"NOT", TokenType::NOT},
    {"CASE", TokenType::CASE}, {"WHEN", TokenType::WHEN}, {"THEN", TokenType::THEN},
    {"ELSE", TokenType::ELSE}, {"END", TokenType::END}, {"BETWEEN", TokenType::BETWEEN},
    {"EXISTS", TokenType::EXISTS}, {"IN", TokenType::IN}, {"COUNT", TokenType::COUNT},
    {"SUM", TokenType::SUM}, {"AVG", TokenType::AVG}, {"MAX", TokenType::MAX},
    {"MIN", TokenType::MIN}, {"COALESCE", TokenType::COALESCE}, {"NOW", TokenType::NOW},
    {"CURRENT_DATE", TokenType::CURRENT_DATE}, {"INTERVAL", TokenType::INTERVAL},
    {"ASC", TokenType::ASC}, {"DESC", TokenType::DESC}
};

SQLLexer::SQLLexer(std::string sql) : input(std::move(sql)), position(0) {
    length = input.length();
}

std::vector<Token> SQLLexer::tokenize() {
    std::vector<Token> tokens;
    position = 0;

    while (position < length) {
        skipWhitespace();
        if (position >= length) break;

        Token token = nextToken();
        tokens.push_back(std::move(token));
    }

    tokens.emplace_back(TokenType::EOF_TOKEN, "", position);
    return tokens;
}

void SQLLexer::skipWhitespace() {
    while (position < length && std::isspace(input[position])) {
        position++;
    }
}

Token SQLLexer::nextToken() {
    size_t start = position;
    const char ch = input[position];

    // Single character tokens
    switch (ch) {
        case '(': position++; return {TokenType::LEFT_PAREN, "(", start};
        case ')': position++; return {TokenType::RIGHT_PAREN, ")", start};
        case ',': position++; return {TokenType::COMMA, ",", start};
        case ';': position++; return {TokenType::SEMICOLON, ";", start};
        case '.': position++; return {TokenType::DOT, ".", start};
        case '+': position++; return {TokenType::PLUS, "+", start};
        case '-': position++; return {TokenType::MINUS, "-", start};
        case '*': position++; return {TokenType::MULTIPLY, "*", start};
        case '/': position++; return {TokenType::DIVIDE, "/", start};
        default: ;
    }

    // Multi-character operators
    if (ch == '=') {
        position++;
        return {TokenType::EQUALS, "=", start};
    }

    if (ch == '!' && position + 1 < length && input[position + 1] == '=') {
        position += 2;
        return {TokenType::NOT_EQUALS, "!=", start};
    }

    if (ch == '<') {
        position++;
        if (position < length && input[position] == '=') {
            position++;
            return {TokenType::LESS_EQUAL, "<=", start};
        }
        return {TokenType::LESS_THAN, "<", start};
    }

    if (ch == '>') {
        position++;
        if (position < length && input[position] == '=') {
            position++;
            return {TokenType::GREATER_EQUAL, ">=", start};
        }
        return {TokenType::GREATER_THAN, ">", start};
    }

    // String literals
    if (ch == '\'' || ch == '"') {
        return parseStringLiteral(ch);
    }

    // Numbers
    if (std::isdigit(ch)) {
        return parseNumber();
    }

    // Identifiers and keywords
    if (std::isalpha(ch) || ch == '_') {
        return parseIdentifierOrKeyword();
    }

    // Unknown character
    position++;
    return {TokenType::UNKNOWN, std::string(1, ch), start};
}

Token SQLLexer::parseStringLiteral(char quote) {
    const size_t start = position;
    position++; // Skip opening quote

    std::string value;
    while (position < length && input[position] != quote) {
        if (input[position] == '\\' && position + 1 < length) {
            position++; // Skip escape character
            value += input[position];
        } else {
            value += input[position];
        }
        position++;
    }

    if (position < length) {
        position++; // Skip closing quote
    }

    return {TokenType::STRING_LITERAL, value, start};
}

Token SQLLexer::parseNumber() {
    size_t start = position;
    std::string value;

    while (position < length && (std::isdigit(input[position]) || input[position] == '.')) {
        value += input[position];
        position++;
    }

    return {TokenType::NUMBER_LITERAL, value, start};
}

Token SQLLexer::parseIdentifierOrKeyword() {
    size_t start = position;
    std::string value;

    while (position < length && (std::isalnum(input[position]) || input[position] == '_')) {
        value += std::toupper(input[position]);
        position++;
    }

    auto it = keywords.find(value);
    TokenType type = (it != keywords.end()) ? it->second : TokenType::IDENTIFIER;

    return {type, value, start};
}

} // namespace sql_parser