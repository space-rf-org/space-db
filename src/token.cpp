#include "sql_parser/token.h"

#include <unordered_set>

namespace sql_parser {

    Token::Token(const TokenType t, std::string v, const size_t pos)
        : type(t), value(std::move(v)), position(pos) {}


    std::string Token::tokenTypeToString(const TokenType type) {
        switch (type) {
            // Core SQL Keywords
            case TokenType::SELECT: return "SELECT";
            case TokenType::FROM: return "FROM";
            case TokenType::WHERE: return "WHERE";
            case TokenType::ORDER: return "ORDER";
            case TokenType::BY: return "BY";
            case TokenType::GROUP: return "GROUP";
            case TokenType::HAVING: return "HAVING";
            case TokenType::LIMIT: return "LIMIT";
            case TokenType::OFFSET: return "OFFSET";
            case TokenType::INSERT: return "INSERT";
            case TokenType::UPDATE: return "UPDATE";
            case TokenType::DELETE: return "DELETE";
            case TokenType::CREATE: return "CREATE";
            case TokenType::DROP: return "DROP";
            case TokenType::ALTER: return "ALTER";
            case TokenType::TRUNCATE: return "TRUNCATE";
            case TokenType::TABLE: return "TABLE";
            case TokenType::VIEW: return "VIEW";
            case TokenType::INDEX: return "INDEX";
            case TokenType::DATABASE: return "DATABASE";
            case TokenType::SCHEMA: return "SCHEMA";
            case TokenType::COLUMN: return "COLUMN";
            case TokenType::CONSTRAINT: return "CONSTRAINT";
            case TokenType::PRIMARY: return "PRIMARY";
            case TokenType::FOREIGN: return "FOREIGN";
            case TokenType::KEY: return "KEY";
            case TokenType::UNIQUE: return "UNIQUE";
            case TokenType::CHECK: return "CHECK";
            case TokenType::DEFAULT: return "DEFAULT";
            case TokenType::NULL_KW: return "NULL";

            // Join Keywords
            case TokenType::JOIN: return "JOIN";
            case TokenType::LEFT: return "LEFT";
            case TokenType::RIGHT: return "RIGHT";
            case TokenType::INNER: return "INNER";
            case TokenType::OUTER: return "OUTER";
            case TokenType::FULL: return "FULL";
            case TokenType::CROSS: return "CROSS";
            case TokenType::NATURAL: return "NATURAL";
            case TokenType::ON: return "ON";
            case TokenType::USING: return "USING";

            // Logical Operators
            case TokenType::AND: return "AND";
            case TokenType::OR: return "OR";
            case TokenType::NOT: return "NOT";
            case TokenType::XOR: return "XOR";

            // Comparison Operators
            case TokenType::EQUALS: return "=";
            case TokenType::NOT_EQUALS: return "!=";
            case TokenType::LESS_THAN: return "<";
            case TokenType::GREATER_THAN: return ">";
            case TokenType::LESS_EQUAL: return "<=";
            case TokenType::GREATER_EQUAL: return ">=";
            case TokenType::IS: return "IS";
            case TokenType::IS_NOT: return "IS NOT";
            case TokenType::LIKE: return "LIKE";
            case TokenType::NOT_LIKE: return "NOT LIKE";
            case TokenType::ILIKE: return "ILIKE";
            case TokenType::SIMILAR: return "SIMILAR";
            case TokenType::REGEXP: return "REGEXP";
            case TokenType::MATCH: return "MATCH";

            // Set Operations
            case TokenType::UNION: return "UNION";
            case TokenType::UNION_ALL: return "UNION ALL";
            case TokenType::INTERSECT: return "INTERSECT";
            case TokenType::EXCEPT: return "EXCEPT";
            case TokenType::MINUS: return "MINUS";

            // Conditional Expressions
            case TokenType::CASE: return "CASE";
            case TokenType::WHEN: return "WHEN";
            case TokenType::THEN: return "THEN";
            case TokenType::ELSE: return "ELSE";
            case TokenType::END: return "END";
            case TokenType::NULLIF: return "NULLIF";
            case TokenType::GREATEST: return "GREATEST";
            case TokenType::LEAST: return "LEAST";

            // Subquery Keywords
            case TokenType::EXISTS: return "EXISTS";
            case TokenType::NOT_EXISTS: return "NOT EXISTS";
            case TokenType::IN: return "IN";
            case TokenType::NOT_IN: return "NOT IN";
            case TokenType::ANY: return "ANY";
            case TokenType::SOME: return "SOME";
            case TokenType::ALL: return "ALL";

            // Range/Pattern Matching
            case TokenType::BETWEEN: return "BETWEEN";
            case TokenType::NOT_BETWEEN: return "NOT BETWEEN";
            case TokenType::ESCAPE: return "ESCAPE";

            // Arithmetic Operators
            case TokenType::PLUS: return "+";
            case TokenType::MULTIPLY: return "*";
            case TokenType::DIVIDE: return "/";
            case TokenType::MODULO: return "%";
            case TokenType::POWER: return "^";

            // String Operators
            case TokenType::CONCAT: return "||";

            // Bitwise Operators
            case TokenType::BIT_AND: return "&";
            case TokenType::BIT_OR: return "|";
            case TokenType::BIT_XOR: return "#";
            case TokenType::BIT_NOT: return "~";
            case TokenType::BIT_SHIFT_LEFT: return "<<";
            case TokenType::BIT_SHIFT_RIGHT: return ">>";

            // JSON Operators
            case TokenType::JSON_EXTRACT: return "->";
            case TokenType::JSON_EXTRACT_TEXT: return "->>";
            case TokenType::JSON_PATH: return "#>";
            case TokenType::JSON_PATH_TEXT: return "#>>";
            case TokenType::JSON_CONTAINS: return "@>";
            case TokenType::JSON_CONTAINED: return "<@";
            case TokenType::JSON_HAS_KEY: return "?";
            case TokenType::JSON_HAS_ANY_KEY: return "?|";
            case TokenType::JSON_HAS_ALL_KEYS: return "?&";

            // Array Operators
            case TokenType::ARRAY_SUBSCRIPT: return "[]";
            case TokenType::ARRAY_SLICE: return "[:]";
            case TokenType::ARRAY_CONCAT: return "||";
            case TokenType::ARRAY_OVERLAP: return "&&";
            case TokenType::ARRAY_CONTAINS: return "@>";
            case TokenType::ARRAY_CONTAINED: return "<@";

            // Assignment Operators
            case TokenType::ASSIGN: return ":=";

            // Aggregate Functions
            case TokenType::COUNT: return "COUNT";
            case TokenType::SUM: return "SUM";
            case TokenType::AVG: return "AVG";
            case TokenType::MIN: return "MIN";
            case TokenType::MAX: return "MAX";
            case TokenType::STDDEV: return "STDDEV";
            case TokenType::VARIANCE: return "VARIANCE";
            case TokenType::STRING_AGG: return "STRING_AGG";
            case TokenType::ARRAY_AGG: return "ARRAY_AGG";
            case TokenType::JSON_AGG: return "JSON_AGG";
            case TokenType::JSON_OBJECT_AGG: return "JSON_OBJECT_AGG";
            case TokenType::BOOL_AND: return "BOOL_AND";
            case TokenType::BOOL_OR: return "BOOL_OR";
            case TokenType::EVERY: return "EVERY";
            case TokenType::PERCENTILE_CONT: return "PERCENTILE_CONT";
            case TokenType::PERCENTILE_DISC: return "PERCENTILE_DISC";

            // Window Functions
            case TokenType::OVER: return "OVER";
            case TokenType::PARTITION: return "PARTITION";
            case TokenType::ROWS: return "ROWS";
            case TokenType::RANGE: return "RANGE";
            case TokenType::UNBOUNDED: return "UNBOUNDED";
            case TokenType::PRECEDING: return "PRECEDING";
            case TokenType::FOLLOWING: return "FOLLOWING";
            case TokenType::CURRENT: return "CURRENT";
            case TokenType::ROW_NUMBER: return "ROW_NUMBER";
            case TokenType::RANK: return "RANK";
            case TokenType::DENSE_RANK: return "DENSE_RANK";
            case TokenType::NTILE: return "NTILE";
            case TokenType::PERCENT_RANK: return "PERCENT_RANK";
            case TokenType::CUME_DIST: return "CUME_DIST";
            case TokenType::LAG: return "LAG";
            case TokenType::LEAD: return "LEAD";
            case TokenType::FIRST_VALUE: return "FIRST_VALUE";
            case TokenType::LAST_VALUE: return "LAST_VALUE";
            case TokenType::NTH_VALUE: return "NTH_VALUE";

            // Mathematical Functions
            case TokenType::ABS: return "ABS";
            case TokenType::CEIL: return "CEIL";
            case TokenType::CEILING: return "CEILING";
            case TokenType::FLOOR: return "FLOOR";
            case TokenType::ROUND: return "ROUND";
            case TokenType::TRUNC: return "TRUNC";
            //case TokenType::TRUNCATE: return "TRUNCATE";
            case TokenType::SQRT: return "SQRT";
            case TokenType::POWER_FUNC: return "POWER";
            case TokenType::EXP: return "EXP";
            case TokenType::LN: return "LN";
            case TokenType::LOG: return "LOG";
            case TokenType::LOG10: return "LOG10";
            case TokenType::SIN: return "SIN";
            case TokenType::COS: return "COS";
            case TokenType::TAN: return "TAN";
            case TokenType::ASIN: return "ASIN";
            case TokenType::ACOS: return "ACOS";
            case TokenType::ATAN: return "ATAN";
            case TokenType::ATAN2: return "ATAN2";
            case TokenType::DEGREES: return "DEGREES";
            case TokenType::RADIANS: return "RADIANS";
            case TokenType::PI: return "PI";
            case TokenType::RANDOM: return "RANDOM";
            case TokenType::SETSEED: return "SETSEED";

            // String Functions
            case TokenType::LENGTH: return "LENGTH";
            case TokenType::CHAR_LENGTH: return "CHAR_LENGTH";
            case TokenType::CHARACTER_LENGTH: return "CHARACTER_LENGTH";
            case TokenType::OCTET_LENGTH: return "OCTET_LENGTH";
            case TokenType::UPPER: return "UPPER";
            case TokenType::LOWER: return "LOWER";
            case TokenType::INITCAP: return "INITCAP";
            case TokenType::TITLE: return "TITLE";
            case TokenType::LTRIM: return "LTRIM";
            case TokenType::RTRIM: return "RTRIM";
            case TokenType::TRIM: return "TRIM";
            case TokenType::BTRIM: return "BTRIM";
            case TokenType::SUBSTRING: return "SUBSTRING";
            case TokenType::SUBSTR: return "SUBSTR";
            case TokenType::LEFT_FUNC: return "LEFT";
            case TokenType::RIGHT_FUNC: return "RIGHT";
            case TokenType::POSITION: return "POSITION";
            case TokenType::STRPOS: return "STRPOS";
            case TokenType::INSTR: return "INSTR";
            case TokenType::REPLACE: return "REPLACE";
            case TokenType::TRANSLATE: return "TRANSLATE";
            case TokenType::OVERLAY: return "OVERLAY";
            case TokenType::CONCAT_FUNC: return "CONCAT";
            case TokenType::CONCAT_WS: return "CONCAT_WS";
            case TokenType::SPLIT_PART: return "SPLIT_PART";
            case TokenType::STRING_TO_ARRAY: return "STRING_TO_ARRAY";
            case TokenType::ARRAY_TO_STRING: return "ARRAY_TO_STRING";
            case TokenType::ASCII: return "ASCII";
            case TokenType::CHR: return "CHR";
            case TokenType::MD5: return "MD5";
            case TokenType::SHA1: return "SHA1";
            case TokenType::SHA256: return "SHA256";
            case TokenType::ENCODE: return "ENCODE";
            case TokenType::DECODE: return "DECODE";
            case TokenType::TO_HEX: return "TO_HEX";

            // Date/Time Functions
            case TokenType::NOW: return "NOW";
            case TokenType::CURRENT_DATE: return "CURRENT_DATE";
            case TokenType::CURRENT_TIME: return "CURRENT_TIME";
            case TokenType::CURRENT_TIMESTAMP: return "CURRENT_TIMESTAMP";
            case TokenType::LOCALTIME: return "LOCALTIME";
            case TokenType::LOCALTIMESTAMP: return "LOCALTIMESTAMP";
            case TokenType::DATE_TRUNC: return "DATE_TRUNC";
            case TokenType::DATE_PART: return "DATE_PART";
            case TokenType::EXTRACT: return "EXTRACT";
            case TokenType::AGE: return "AGE";
            case TokenType::JUSTIFY_DAYS: return "JUSTIFY_DAYS";
            case TokenType::JUSTIFY_HOURS: return "JUSTIFY_HOURS";
            case TokenType::JUSTIFY_INTERVAL: return "JUSTIFY_INTERVAL";
            case TokenType::MAKE_DATE: return "MAKE_DATE";
            case TokenType::MAKE_TIME: return "MAKE_TIME";
            case TokenType::MAKE_TIMESTAMP: return "MAKE_TIMESTAMP";
            case TokenType::MAKE_TIMESTAMPTZ: return "MAKE_TIMESTAMPTZ";
            case TokenType::TO_DATE: return "TO_DATE";
            case TokenType::TO_TIMESTAMP: return "TO_TIMESTAMP";
            case TokenType::TO_CHAR: return "TO_CHAR";
            case TokenType::CLOCK_TIMESTAMP: return "CLOCK_TIMESTAMP";
            case TokenType::STATEMENT_TIMESTAMP: return "STATEMENT_TIMESTAMP";
            case TokenType::TRANSACTION_TIMESTAMP: return "TRANSACTION_TIMESTAMP";

            // Type Conversion Functions
            case TokenType::CAST: return "CAST";
            case TokenType::CONVERT: return "CONVERT";
            case TokenType::TO_NUMBER: return "TO_NUMBER";
            case TokenType::TO_CHAR_FUNC: return "TO_CHAR";

            // Null Handling Functions
            case TokenType::COALESCE: return "COALESCE";
            case TokenType::ISNULL: return "ISNULL";
            case TokenType::IFNULL: return "IFNULL";
            case TokenType::NVL: return "NVL";
            case TokenType::NVL2: return "NVL2";

            // Data Types
            case TokenType::INTEGER: return "INTEGER";
            case TokenType::INT: return "INT";
            case TokenType::BIGINT: return "BIGINT";
            case TokenType::SMALLINT: return "SMALLINT";
            case TokenType::TINYINT: return "TINYINT";
            case TokenType::DECIMAL: return "DECIMAL";
            case TokenType::NUMERIC: return "NUMERIC";
            case TokenType::REAL: return "REAL";
            case TokenType::FLOAT: return "FLOAT";
            case TokenType::DOUBLE: return "DOUBLE";
            case TokenType::PRECISION: return "PRECISION";
            case TokenType::CHAR: return "CHAR";
            case TokenType::VARCHAR: return "VARCHAR";
            case TokenType::TEXT: return "TEXT";
            case TokenType::CLOB: return "CLOB";
            case TokenType::BINARY: return "BINARY";
            case TokenType::VARBINARY: return "VARBINARY";
            case TokenType::BLOB: return "BLOB";
            case TokenType::BOOLEAN: return "BOOLEAN";
            case TokenType::BOOL: return "BOOL";
            case TokenType::DATE: return "DATE";
            case TokenType::TIME: return "TIME";
            case TokenType::TIMESTAMP: return "TIMESTAMP";
            case TokenType::TIMESTAMPTZ: return "TIMESTAMPTZ";
            case TokenType::TIMETZ: return "TIMETZ";
            case TokenType::INTERVAL: return "INTERVAL";
            case TokenType::UUID: return "UUID";
            case TokenType::JSON: return "JSON";
            case TokenType::JSONB: return "JSONB";
            case TokenType::XML: return "XML";
            case TokenType::ARRAY: return "ARRAY";
            case TokenType::ENUM: return "ENUM";

            // PostgreSQL Specific Types
            case TokenType::SERIAL: return "SERIAL";
            case TokenType::BIGSERIAL: return "BIGSERIAL";
            case TokenType::SMALLSERIAL: return "SMALLSERIAL";
            case TokenType::INET: return "INET";
            case TokenType::CIDR: return "CIDR";
            case TokenType::MACADDR: return "MACADDR";
            case TokenType::MACADDR8: return "MACADDR8";
            case TokenType::POINT: return "POINT";
            case TokenType::LINE: return "LINE";
            case TokenType::LSEG: return "LSEG";
            case TokenType::BOX: return "BOX";
            case TokenType::PATH: return "PATH";
            case TokenType::POLYGON: return "POLYGON";
            case TokenType::CIRCLE: return "CIRCLE";
            case TokenType::TSQUERY: return "TSQUERY";
            case TokenType::TSVECTOR: return "TSVECTOR";
            case TokenType::INT4RANGE: return "INT4RANGE";
            case TokenType::INT8RANGE: return "INT8RANGE";
            case TokenType::NUMRANGE: return "NUMRANGE";
            case TokenType::TSRANGE: return "TSRANGE";
            case TokenType::TSTZRANGE: return "TSTZRANGE";
            case TokenType::DATERANGE: return "DATERANGE";
            case TokenType::MONEY: return "MONEY";
            case TokenType::OID: return "OID";

            // DuckDB Specific Types
            case TokenType::HUGEINT: return "HUGEINT";
            case TokenType::UTINYINT: return "UTINYINT";
            case TokenType::USMALLINT: return "USMALLINT";
            case TokenType::UINTEGER: return "UINTEGER";
            case TokenType::UBIGINT: return "UBIGINT";
            case TokenType::MAP: return "MAP";
            case TokenType::STRUCT: return "STRUCT";
            case TokenType::LIST: return "LIST";

            // Constraints and Modifiers
            case TokenType::NOT_NULL: return "NOT NULL";
            case TokenType::AUTO_INCREMENT: return "AUTO_INCREMENT";
            case TokenType::AUTOINCREMENT: return "AUTOINCREMENT";
            case TokenType::IDENTITY: return "IDENTITY";
            case TokenType::REFERENCES: return "REFERENCES";
            case TokenType::DEFERRABLE: return "DEFERRABLE";
            case TokenType::INITIALLY: return "INITIALLY";
            case TokenType::DEFERRED: return "DEFERRED";
            case TokenType::IMMEDIATE: return "IMMEDIATE";
            case TokenType::MATCH_FULL: return "MATCH FULL";
            case TokenType::MATCH_PARTIAL: return "MATCH PARTIAL";
            case TokenType::MATCH_SIMPLE: return "MATCH SIMPLE";
            case TokenType::CASCADE: return "CASCADE";
            case TokenType::RESTRICT: return "RESTRICT";
            case TokenType::SET_NULL: return "SET NULL";
            case TokenType::SET_DEFAULT: return "SET DEFAULT";
            case TokenType::NO_ACTION: return "NO ACTION";

            // Transaction Keywords
            case TokenType::BEGIN: return "BEGIN";
            case TokenType::START: return "START";
            case TokenType::TRANSACTION: return "TRANSACTION";
            case TokenType::WORK: return "WORK";
            case TokenType::COMMIT: return "COMMIT";
            case TokenType::ROLLBACK: return "ROLLBACK";
            case TokenType::SAVEPOINT: return "SAVEPOINT";
            case TokenType::RELEASE: return "RELEASE";
            case TokenType::ISOLATION: return "ISOLATION";
            case TokenType::LEVEL: return "LEVEL";
            case TokenType::READ: return "READ";
            case TokenType::WRITE: return "WRITE";
            case TokenType::ONLY: return "ONLY";
            case TokenType::UNCOMMITTED: return "UNCOMMITTED";
            case TokenType::COMMITTED: return "COMMITTED";
            case TokenType::REPEATABLE: return "REPEATABLE";
            case TokenType::SERIALIZABLE: return "SERIALIZABLE";

            // Index Keywords
            case TokenType::BTREE: return "BTREE";
            case TokenType::HASH: return "HASH";
            case TokenType::GIST: return "GIST";
            case TokenType::SPGIST: return "SPGIST";
            case TokenType::GIN: return "GIN";
            case TokenType::BRIN: return "BRIN";
            case TokenType::CONCURRENTLY: return "CONCURRENTLY";
            case TokenType::IF_NOT_EXISTS: return "IF NOT EXISTS";
            case TokenType::IF_EXISTS: return "IF EXISTS";

            // Window Clause Keywords
            case TokenType::EXCLUDE: return "EXCLUDE";
            case TokenType::TIES: return "TIES";
            case TokenType::NO_OTHERS: return "NO OTHERS";
            case TokenType::GROUPS: return "GROUPS";

            // Set Operations Modifiers
            case TokenType::DISTINCT: return "DISTINCT";

            // Common Table Expressions
            case TokenType::WITH: return "WITH";
            case TokenType::RECURSIVE: return "RECURSIVE";
            case TokenType::AS: return "AS";

            // Sorting Keywords
            case TokenType::ASC: return "ASC";
            case TokenType::DESC: return "DESC";
            case TokenType::NULLS: return "NULLS";
            case TokenType::FIRST: return "FIRST";
            case TokenType::LAST: return "LAST";

            // Function Keywords
            case TokenType::FUNCTION: return "FUNCTION";
            case TokenType::PROCEDURE: return "PROCEDURE";
            case TokenType::RETURNS: return "RETURNS";
            case TokenType::RETURN: return "RETURN";
            case TokenType::LANGUAGE: return "LANGUAGE";
            case TokenType::IMMUTABLE: return "IMMUTABLE";
            case TokenType::STABLE: return "STABLE";
            case TokenType::VOLATILE: return "VOLATILE";
            case TokenType::STRICT: return "STRICT";
            case TokenType::CALLED: return "CALLED";
            case TokenType::INPUT: return "INPUT";
            case TokenType::SECURITY: return "SECURITY";
            case TokenType::DEFINER: return "DEFINER";
            case TokenType::INVOKER: return "INVOKER";
            case TokenType::PARALLEL: return "PARALLEL";
            case TokenType::SAFE: return "SAFE";
            case TokenType::RESTRICTED: return "RESTRICTED";
            case TokenType::UNSAFE: return "UNSAFE";

            // Administrative Keywords
            case TokenType::ANALYZE: return "ANALYZE";
            case TokenType::VACUUM: return "VACUUM";
            case TokenType::REINDEX: return "REINDEX";
            case TokenType::CLUSTER: return "CLUSTER";
            case TokenType::EXPLAIN: return "EXPLAIN";
            case TokenType::DESCRIBE: return "DESCRIBE";
            case TokenType::DESC_KW: return "DESC";
            case TokenType::GRANT: return "GRANT";
            case TokenType::REVOKE: return "REVOKE";
            case TokenType::ROLE: return "ROLE";
            case TokenType::USER: return "USER";
            case TokenType::GROUP_ROLE: return "GROUP";
            case TokenType::USAGE: return "USAGE";
            case TokenType::EXECUTE: return "EXECUTE";
            case TokenType::CONNECT: return "CONNECT";
            case TokenType::TEMPORARY: return "TEMPORARY";
            case TokenType::TEMP: return "TEMP";

            // Special Literals and Identifiers
            case TokenType::TRUE_LIT: return "TRUE";
            case TokenType::FALSE_LIT: return "FALSE";
            case TokenType::NULL_LIT: return "NULL";
            case TokenType::CURRENT_USER: return "CURRENT_USER";
            case TokenType::SESSION_USER: return "SESSION_USER";

            // Wildcard and Special Operators
            case TokenType::ASTERISK: return "*";
            case TokenType::QUESTION: return "?";
            case TokenType::DOLLAR: return "$";
            case TokenType::AT_SIGN: return "@";

            // Delimiters and Punctuation
            case TokenType::LEFT_PAREN: return "(";
            case TokenType::RIGHT_PAREN: return ")";
            case TokenType::LEFT_BRACKET: return "[";
            case TokenType::RIGHT_BRACKET: return "]";
            case TokenType::LEFT_BRACE: return "{";
            case TokenType::RIGHT_BRACE: return "}";
            case TokenType::COMMA: return ",";
            case TokenType::SEMICOLON: return ";";
            case TokenType::DOT: return ".";
            case TokenType::DOUBLE_DOT: return "..";
            case TokenType::COLON: return ":";
            case TokenType::DOUBLE_COLON: return "::";

            // Quote Types
            case TokenType::SINGLE_QUOTE: return "'";
            case TokenType::DOUBLE_QUOTE: return "\"";
            case TokenType::BACKTICK: return "`";
            case TokenType::DOLLAR_QUOTE: return "$$";

            // Comments
            case TokenType::LINE_COMMENT: return "-- comment";
            case TokenType::BLOCK_COMMENT: return "/* comment */";

            // Literals
            case TokenType::STRING_LITERAL: return "STRING_LITERAL";
            case TokenType::NUMBER_LITERAL: return "NUMBER_LITERAL";
            case TokenType::INTEGER_LITERAL: return "INTEGER_LITERAL";
            case TokenType::FLOAT_LITERAL: return "FLOAT_LITERAL";
            case TokenType::BINARY_LITERAL: return "BINARY_LITERAL";
            case TokenType::HEX_LITERAL: return "HEX_LITERAL";
            case TokenType::UNICODE_LITERAL: return "UNICODE_LITERAL";
            case TokenType::IDENTIFIER: return "IDENTIFIER";
            case TokenType::QUOTED_IDENTIFIER: return "QUOTED_IDENTIFIER";

            // Special Tokens
            case TokenType::WHITESPACE: return "WHITESPACE";
            case TokenType::NEWLINE: return "NEWLINE";
            case TokenType::EOF_TOKEN: return "EOF";
            case TokenType::UNKNOWN: return "UNKNOWN";

            // Error Recovery Tokens
            case TokenType::UNEXPECTED_CHAR: return "UNEXPECTED_CHAR";
            case TokenType::UNTERMINATED_STRING: return "UNTERMINATED_STRING";
            case TokenType::UNTERMINATED_COMMENT: return "UNTERMINATED_COMMENT";
            case TokenType::INVALID_NUMBER: return "INVALID_NUMBER";

            // Preprocessor-like
            case TokenType::INCLUDE: return "INCLUDE";
            case TokenType::DEFINE: return "DEFINE";
            case TokenType::UNDEF: return "UNDEF";

            // Advanced PostgreSQL Features
            case TokenType::VARIADIC: return "VARIADIC";
            case TokenType::INOUT: return "INOUT";
            case TokenType::OUT_PARAM: return "OUT";
            case TokenType::TRIGGER: return "TRIGGER";
            case TokenType::BEFORE: return "BEFORE";
            case TokenType::AFTER: return "AFTER";
            case TokenType::INSTEAD: return "INSTEAD";
            case TokenType::OF: return "OF";
            case TokenType::FOR: return "FOR";
            case TokenType::EACH: return "EACH";
            case TokenType::ROW: return "ROW";
            case TokenType::STATEMENT: return "STATEMENT";
            case TokenType::OLD: return "OLD";
            case TokenType::NEW: return "NEW";
            case TokenType::REFERENCING: return "REFERENCING";
            case TokenType::WHEN_CLAUSE: return "WHEN";

            // DuckDB Specific Extensions
            case TokenType::PRAGMA: return "PRAGMA";
            case TokenType::INSTALL: return "INSTALL";
            case TokenType::LOAD: return "LOAD";
            case TokenType::FROM_CSV: return "FROM_CSV";
            case TokenType::FROM_PARQUET: return "FROM_PARQUET";
            case TokenType::FROM_JSON: return "FROM_JSON";
            case TokenType::COPY_TO: return "COPY_TO";
            case TokenType::COPY_FROM: return "COPY_FROM";

            // SQLite Specific Features
            case TokenType::ATTACH: return "ATTACH";
            case TokenType::DETACH: return "DETACH";
            case TokenType::WITHOUT_ROWID: return "WITHOUT ROWID";
            case TokenType::VIRTUAL: return "VIRTUAL";

            // Advanced SQL Features
            case TokenType::LATERAL: return "LATERAL";
            case TokenType::GROUPING: return "GROUPING";
            case TokenType::ROLLUP: return "ROLLUP";
            case TokenType::CUBE: return "CUBE";
            case TokenType::GROUPING_SETS: return "GROUPING SETS";
            case TokenType::FILTER: return "FILTER";

            // Regular Expression Operators
            case TokenType::REGEX_MATCH: return "~";
            case TokenType::REGEX_NOT_MATCH: return "!~";
            case TokenType::REGEX_MATCH_CI: return "~*";
            case TokenType::REGEX_NOT_MATCH_CI: return "!~*";

            default:
                return "UNKNOWN_TOKEN_TYPE";
        }
    }

    bool Token::isKeyword(const TokenType type) {
        static const std::unordered_set keywords = {
            // Core SQL Keywords
            TokenType::SELECT, TokenType::FROM, TokenType::WHERE, TokenType::ORDER, TokenType::BY,
            TokenType::GROUP, TokenType::HAVING, TokenType::LIMIT, TokenType::OFFSET,
            TokenType::INSERT, TokenType::UPDATE, TokenType::DELETE, TokenType::CREATE,
            TokenType::DROP, TokenType::ALTER, TokenType::TRUNCATE, TokenType::TABLE,
            TokenType::VIEW, TokenType::INDEX, TokenType::DATABASE, TokenType::SCHEMA,
            TokenType::COLUMN, TokenType::CONSTRAINT, TokenType::PRIMARY, TokenType::FOREIGN,
            TokenType::KEY, TokenType::UNIQUE, TokenType::CHECK, TokenType::DEFAULT, TokenType::NULL_KW,

            // Join Keywords
            TokenType::JOIN, TokenType::LEFT, TokenType::RIGHT, TokenType::INNER, TokenType::OUTER,
            TokenType::FULL, TokenType::CROSS, TokenType::NATURAL, TokenType::ON, TokenType::USING,

            // Logical Keywords
            TokenType::AND, TokenType::OR, TokenType::NOT, TokenType::XOR,

            // Comparison Keywords
            TokenType::IS, TokenType::IS_NOT, TokenType::LIKE, TokenType::NOT_LIKE,
            TokenType::ILIKE, TokenType::SIMILAR, TokenType::REGEXP, TokenType::MATCH,

            // Set Operations
            TokenType::UNION, TokenType::UNION_ALL, TokenType::INTERSECT, TokenType::EXCEPT, TokenType::MINUS,

            // Conditional Keywords
            TokenType::CASE, TokenType::WHEN, TokenType::THEN, TokenType::ELSE, TokenType::END,

            // Subquery Keywords
            TokenType::EXISTS, TokenType::NOT_EXISTS, TokenType::IN, TokenType::NOT_IN,
            TokenType::ANY, TokenType::SOME, TokenType::ALL,

            // Range Keywords
            TokenType::BETWEEN, TokenType::NOT_BETWEEN, TokenType::ESCAPE,

            // Data Types
            TokenType::INTEGER, TokenType::INT, TokenType::BIGINT, TokenType::SMALLINT, TokenType::TINYINT,
            TokenType::DECIMAL, TokenType::NUMERIC, TokenType::REAL, TokenType::FLOAT, TokenType::DOUBLE,
            TokenType::PRECISION, TokenType::CHAR, TokenType::VARCHAR, TokenType::TEXT, TokenType::CLOB,
            TokenType::BINARY, TokenType::VARBINARY, TokenType::BLOB, TokenType::BOOLEAN, TokenType::BOOL,
            TokenType::DATE, TokenType::TIME, TokenType::TIMESTAMP, TokenType::TIMESTAMPTZ, TokenType::TIMETZ,
            TokenType::INTERVAL, TokenType::UUID, TokenType::JSON, TokenType::JSONB, TokenType::XML,
            TokenType::ARRAY, TokenType::ENUM,

            // PostgreSQL Specific Types
            TokenType::SERIAL, TokenType::BIGSERIAL, TokenType::SMALLSERIAL, TokenType::INET, TokenType::CIDR,
            TokenType::MACADDR, TokenType::MACADDR8, TokenType::POINT, TokenType::LINE, TokenType::LSEG,
            TokenType::BOX, TokenType::PATH, TokenType::POLYGON, TokenType::CIRCLE, TokenType::TSQUERY,
            TokenType::TSVECTOR, TokenType::INT4RANGE, TokenType::INT8RANGE, TokenType::NUMRANGE,
            TokenType::TSRANGE, TokenType::TSTZRANGE, TokenType::DATERANGE, TokenType::MONEY, TokenType::OID,

            // DuckDB Specific Types
            TokenType::HUGEINT, TokenType::UTINYINT, TokenType::USMALLINT, TokenType::UINTEGER,
            TokenType::UBIGINT, TokenType::MAP, TokenType::STRUCT, TokenType::LIST,

            // Constraints
            TokenType::NOT_NULL, TokenType::AUTO_INCREMENT, TokenType::AUTOINCREMENT, TokenType::IDENTITY,
            TokenType::REFERENCES, TokenType::DEFERRABLE, TokenType::INITIALLY, TokenType::DEFERRED,
            TokenType::IMMEDIATE, TokenType::MATCH_FULL, TokenType::MATCH_PARTIAL, TokenType::MATCH_SIMPLE,
            TokenType::CASCADE, TokenType::RESTRICT, TokenType::SET_NULL, TokenType::SET_DEFAULT, TokenType::NO_ACTION,

            // Transaction Keywords
            TokenType::BEGIN, TokenType::START, TokenType::TRANSACTION, TokenType::WORK,
            TokenType::COMMIT, TokenType::ROLLBACK, TokenType::SAVEPOINT, TokenType::RELEASE,
            TokenType::ISOLATION, TokenType::LEVEL, TokenType::READ, TokenType::WRITE, TokenType::ONLY,
            TokenType::UNCOMMITTED, TokenType::COMMITTED, TokenType::REPEATABLE, TokenType::SERIALIZABLE,

            // Index Keywords
            TokenType::BTREE, TokenType::HASH, TokenType::GIST, TokenType::SPGIST, TokenType::GIN, TokenType::BRIN,
            TokenType::CONCURRENTLY, TokenType::IF_NOT_EXISTS, TokenType::IF_EXISTS,

            // Window Keywords
            TokenType::OVER, TokenType::PARTITION, TokenType::ROWS, TokenType::RANGE, TokenType::UNBOUNDED,
            TokenType::PRECEDING, TokenType::FOLLOWING, TokenType::CURRENT, TokenType::EXCLUDE,
            TokenType::TIES, TokenType::NO_OTHERS, TokenType::GROUPS,

            // Modifiers
            TokenType::DISTINCT, TokenType::ALL,

            // CTE Keywords
            TokenType::WITH, TokenType::RECURSIVE, TokenType::AS,

            // Sorting
            TokenType::ASC, TokenType::DESC, TokenType::NULLS, TokenType::FIRST, TokenType::LAST,

            // Function Keywords
            TokenType::FUNCTION, TokenType::PROCEDURE, TokenType::RETURNS, TokenType::RETURN,
            TokenType::LANGUAGE, TokenType::IMMUTABLE, TokenType::STABLE, TokenType::VOLATILE,
            TokenType::STRICT, TokenType::CALLED, TokenType::INPUT, TokenType::SECURITY,
            TokenType::DEFINER, TokenType::INVOKER, TokenType::PARALLEL, TokenType::SAFE,
            TokenType::RESTRICTED, TokenType::UNSAFE,

            // Administrative
            TokenType::ANALYZE, TokenType::VACUUM, TokenType::REINDEX, TokenType::CLUSTER,
            TokenType::EXPLAIN, TokenType::DESCRIBE, TokenType::DESC_KW, TokenType::GRANT, TokenType::REVOKE,
            TokenType::ROLE, TokenType::USER, TokenType::GROUP_ROLE, TokenType::USAGE, TokenType::EXECUTE,
            TokenType::CONNECT, TokenType::TEMPORARY, TokenType::TEMP,

            // Special Keywords
            TokenType::CURRENT_USER, TokenType::SESSION_USER,

            // Type Conversion
            TokenType::CAST, TokenType::CONVERT,

            // Advanced PostgreSQL
            TokenType::VARIADIC, TokenType::INOUT, TokenType::OUT_PARAM, TokenType::TRIGGER,
            TokenType::BEFORE, TokenType::AFTER, TokenType::INSTEAD, TokenType::OF, TokenType::FOR,
            TokenType::EACH, TokenType::ROW, TokenType::STATEMENT, TokenType::OLD, TokenType::NEW,
            TokenType::REFERENCING, TokenType::WHEN_CLAUSE,

            // DuckDB Extensions
            TokenType::PRAGMA, TokenType::INSTALL, TokenType::LOAD, TokenType::FROM_CSV,
            TokenType::FROM_PARQUET, TokenType::FROM_JSON, TokenType::COPY_TO, TokenType::COPY_FROM,

            // SQLite Specific
            TokenType::ATTACH, TokenType::DETACH, TokenType::WITHOUT_ROWID, TokenType::VIRTUAL,

            // Advanced SQL
            TokenType::LATERAL, TokenType::GROUPING, TokenType::ROLLUP, TokenType::CUBE,
            TokenType::GROUPING_SETS, TokenType::FILTER,

            // Preprocessor
            TokenType::INCLUDE, TokenType::DEFINE, TokenType::UNDEF
        };

        return keywords.find(type) != keywords.end();
    }

    bool Token::isOperator(const TokenType type) {
        static const std::unordered_set<TokenType> operators = {
            // Comparison Operators
            TokenType::EQUALS, TokenType::NOT_EQUALS, TokenType::LESS_THAN, TokenType::GREATER_THAN,
            TokenType::LESS_EQUAL, TokenType::GREATER_EQUAL,

            // Arithmetic Operators
            TokenType::PLUS, TokenType::MINUS, TokenType::MULTIPLY, TokenType::DIVIDE,
            TokenType::MODULO, TokenType::POWER,

            // String Operators
            TokenType::CONCAT,

            // Bitwise Operators
            TokenType::BIT_AND, TokenType::BIT_OR, TokenType::BIT_XOR, TokenType::BIT_NOT,
            TokenType::BIT_SHIFT_LEFT, TokenType::BIT_SHIFT_RIGHT,

            // JSON Operators
            TokenType::JSON_EXTRACT, TokenType::JSON_EXTRACT_TEXT, TokenType::JSON_PATH,
            TokenType::JSON_PATH_TEXT, TokenType::JSON_CONTAINS, TokenType::JSON_CONTAINED,
            TokenType::JSON_HAS_KEY, TokenType::JSON_HAS_ANY_KEY, TokenType::JSON_HAS_ALL_KEYS,

            // Array Operators
            TokenType::ARRAY_SUBSCRIPT, TokenType::ARRAY_SLICE, TokenType::ARRAY_CONCAT,
            TokenType::ARRAY_OVERLAP, TokenType::ARRAY_CONTAINS, TokenType::ARRAY_CONTAINED,

            // Assignment Operators
            TokenType::ASSIGN,

            // Regular Expression Operators
            TokenType::REGEX_MATCH, TokenType::REGEX_NOT_MATCH, TokenType::REGEX_MATCH_CI,
            TokenType::REGEX_NOT_MATCH_CI,

            // Special Operators
            TokenType::ASTERISK, TokenType::QUESTION, TokenType::DOLLAR, TokenType::AT_SIGN,
            TokenType::DOUBLE_COLON
        };

        return operators.find(type) != operators.end();
    }

    bool Token::isLiteral(const TokenType type) {
        static const std::unordered_set<TokenType> literals = {
            // Basic Literals
            TokenType::STRING_LITERAL, TokenType::NUMBER_LITERAL, TokenType::INTEGER_LITERAL,
            TokenType::FLOAT_LITERAL, TokenType::BINARY_LITERAL, TokenType::HEX_LITERAL,
            TokenType::UNICODE_LITERAL,

            // Boolean Literals
            TokenType::TRUE_LIT, TokenType::FALSE_LIT, TokenType::NULL_LIT,

            // Identifiers
            TokenType::IDENTIFIER, TokenType::QUOTED_IDENTIFIER
        };

        return literals.find(type) != literals.end();
    }

    bool Token::isReservedKeyword(const std::string& word) {
    // Convert to uppercase for case-insensitive comparison
    std::string upperWord = word;
    std::transform(upperWord.begin(), upperWord.end(), upperWord.begin(), ::toupper);

    // Use static unordered_set for O(1) lookup performance
    static const std::unordered_set<std::string> keywords = {
        // Basic SQL keywords
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET",

        // JOIN keywords
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON", "USING",

        // Logical operators
        "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL",

        // CASE expressions
        "CASE", "WHEN", "THEN", "ELSE", "END",

        // Other common keywords
        "AS", "DISTINCT", "ALL", "ANY", "SOME", "UNION", "INTERSECT", "EXCEPT",

        // DML keywords
        "INSERT", "UPDATE", "DELETE", "MERGE", "INTO", "VALUES", "SET",

        // DDL keywords
        "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "PROCEDURE", "FUNCTION",
        "TRIGGER", "CONSTRAINT", "PRIMARY", "FOREIGN", "KEY", "REFERENCES",
        "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT", "IDENTITY",

        // Data types (common ones)
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC",
        "FLOAT", "REAL", "DOUBLE", "PRECISION", "CHAR", "VARCHAR", "TEXT",
        "NCHAR", "NVARCHAR", "NTEXT", "BINARY", "VARBINARY", "IMAGE",
        "DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR", "BOOLEAN", "BOOL",
        "BLOB", "CLOB", "JSON", "XML", "UUID", "GEOMETRY",

        // Constraint keywords
        "CONSTRAINT", "UNIQUE", "PRIMARY", "FOREIGN", "KEY", "REFERENCES",
        "CHECK", "DEFAULT", "NOT", "NULL", "AUTO_INCREMENT", "IDENTITY",

        // Transaction keywords
        "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "SAVEPOINT",

        // Aggregate functions (treated as reserved)
        "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE",

        // String functions
        "SUBSTRING", "TRIM", "UPPER", "LOWER", "CONCAT", "LENGTH", "CHAR_LENGTH",
        "LEFT", "RIGHT", "LTRIM", "RTRIM", "REPLACE", "REVERSE", "SPACE",

        // Date/Time functions
        "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "EXTRACT", "DATE_ADD", "DATE_SUB", "DATEDIFF", "DATEPART",

        // Mathematical functions
        "ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "POWER", "EXP",
        "LOG", "LOG10", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2",
        "DEGREES", "RADIANS", "PI", "RAND", "SIGN", "MOD",

        // Conditional functions
        "COALESCE", "NULLIF", "ISNULL", "IFNULL", "IF", "IIF", "GREATEST", "LEAST",

        // Conversion functions
        "CAST", "CONVERT", "PARSE", "TRY_CAST", "TRY_CONVERT", "TRY_PARSE",

        // Window functions
        "OVER", "PARTITION", "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
        "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",

        // Set operations
        "UNION", "INTERSECT", "EXCEPT", "MINUS",

        // Subquery keywords
        "ANY", "ALL", "SOME", "EXISTS",

        // Conditional logic
        "IF", "ELSE", "ELSEIF", "ENDIF",

        // Data modification
        "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE",
        "INTO", "VALUES", "SET", "OUTPUT", "RETURNING",

        // Schema operations
        "SCHEMA", "DATABASE", "CATALOG", "OWNER", "GRANT", "REVOKE",
        "PRIVILEGE", "PRIVILEGES", "PERMISSION", "PERMISSIONS",
        "ROLE", "USER", "LOGIN", "PASSWORD",

        // Index operations
        "INDEX", "CLUSTERED", "NONCLUSTERED", "BTREE", "HASH", "BITMAP",

        // Storage keywords
        "TABLESPACE", "FILEGROUP", "PARTITION", "SUBPARTITION",

        // Optimizer hints (common ones)
        "HINT", "FORCE", "USE", "IGNORE", "STRAIGHT_JOIN",

        // Flow control
        "WHILE", "LOOP", "FOR", "FOREACH", "REPEAT", "UNTIL", "CONTINUE", "BREAK",
        "RETURN", "EXIT", "GOTO", "LABEL",

        // Exception handling
        "TRY", "CATCH", "THROW", "RAISERROR", "ERROR",

        // Cursor operations
        "CURSOR", "DECLARE", "OPEN", "FETCH", "CLOSE", "DEALLOCATE",

        // Variable operations
        "DECLARE", "SET", "SELECT", "EXEC", "EXECUTE", "CALL",

        // Literals
        "TRUE", "FALSE", "NULL", "UNKNOWN",

        // Comparison operators (word forms)
        "LIKE", "ILIKE", "SIMILAR", "REGEXP", "RLIKE", "MATCH", "AGAINST",

        // Time intervals
        "INTERVAL", "YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
        "MICROSECOND", "QUARTER", "YEARS", "MONTHS", "WEEKS", "DAYS",
        "HOURS", "MINUTES", "SECONDS", "MICROSECONDS", "QUARTERS",

        // Common SQL Server keywords
        "IDENTITY", "ROWVERSION", "UNIQUEIDENTIFIER", "MONEY", "SMALLMONEY",
        "DATETIME2", "DATETIMEOFFSET", "SMALLDATETIME", "SQL_VARIANT",

        // Common MySQL keywords
        "AUTO_INCREMENT", "UNSIGNED", "ZEROFILL", "ENUM", "SET",
        "MEDIUMINT", "MEDIUMTEXT", "LONGTEXT", "TINYTEXT", "MEDIUMBLOB", "LONGBLOB",

        // Common PostgreSQL keywords
        "SERIAL", "BIGSERIAL", "SMALLSERIAL", "BYTEA", "INET", "CIDR", "MACADDR",
        "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE",

        // Common Oracle keywords
        "NUMBER", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "BFILE", "RAW", "LONG",
        "ROWID", "UROWID", "XMLTYPE", "SYSDATE", "SYSTIMESTAMP", "DUAL",

        // Additional reserved words that might be used as identifiers
        "ADMIN", "AFTER", "AGGREGATE", "ALIAS", "ALLOCATE", "ARE", "ARRAY", "ASENSITIVE",
        "ASSERTION", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "BEFORE", "BINARY",
        "BIT", "BREADTH", "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CATALOG",
        "CHARACTER", "CLASS", "COLLATE", "COLLATION", "COLLECT", "COMPLETION", "CONDITION",
        "CONNECT", "CONNECTION", "CONSTRAINTS", "CONSTRUCTOR", "CORRESPONDING", "CUBE",
        "CURRENT", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
        "CYCLE", "DATA", "DEPTH", "DEREF", "DESCRIBE", "DESCRIPTOR", "DESTROY", "DESTRUCTOR",
        "DETERMINISTIC", "DIAGNOSTICS", "DICTIONARY", "DISCONNECT", "DOMAIN", "DYNAMIC",
        "EACH", "ELEMENT", "EQUALS", "ESCAPE", "EVERY", "EXCEPTION", "EXTERNAL", "FALSE",
        "FILTER", "FIRST", "FREE", "FULLTEXTTABLE", "FUSION", "GENERAL", "GET", "GLOBAL",
        "GO", "GROUPING", "HOLD", "HOST", "IGNORE", "IMMEDIATE", "INDICATOR", "INITIALIZE",
        "INITIALLY", "INOUT", "INPUT", "INTERSECTION", "ISOLATION", "ITERATE", "LANGUAGE",
        "LARGE", "LAST", "LATERAL", "LEADING", "LESS", "LEVEL", "LIKE_REGEX", "LOCAL",
        "LOCATOR", "MAP", "MATCH", "MEMBER", "METHOD", "MODULE", "MULTISET", "NAMES",
        "NATURAL", "NESTING", "NEW", "NONE", "NORMALIZE", "OBJECT", "OCCURRENCES_REGEX",
        "OLD", "ONLY", "OPERATION", "ORDINALITY", "OUT", "OVERLAY", "PAD", "PARAMETER",
        "PARAMETERS", "PARTIAL", "PATH", "POSTFIX", "PREFIX", "PREORDER", "PREPARE",
        "PRESERVE", "PRIOR", "RELATIVE", "RESULT", "RETURNS", "ROLLUP", "ROUTINE", "ROW",
        "ROWS", "SCOPE", "SCROLL", "SEARCH", "SECTION", "SENSITIVE", "SEQUENCE", "SESSION",
        "SETS", "SIMILAR", "SIZE", "SPACE", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION",
        "SQLSTATE", "SQLWARNING", "START", "STATE", "STATIC", "STRUCTURE", "SUBMULTISET",
        "SYMMETRIC", "SYSTEM", "TEMPORARY", "TERMINATE", "THAN", "TRANSLATE", "TRANSLATION",
        "TREAT", "TRUE", "UESCAPE", "UNDER", "UNKNOWN", "UNNEST", "USAGE", "USING", "VALUE",
        "VARIABLE", "WHENEVER", "WHERE", "WIDTH", "WINDOW", "WITHIN", "WITHOUT", "WORK",
        "WRITE", "ZONE"
    };

    return keywords.find(upperWord) != keywords.end();
}

bool Token::isFunction(const std::string& name) {
    // Add more function names as needed
    std::vector<std::string> functions = {
        "ABS", "CEIL", "FLOOR", "ROUND", "SQRT", "POWER",
        "UPPER", "LOWER", "LENGTH", "SUBSTRING", "TRIM",
        "CONCAT", "LEFT", "RIGHT", "REPLACE",
        "DATE", "TIME", "TIMESTAMP", "EXTRACT",
        "IFNULL", "NULLIF", "GREATEST", "LEAST"
    };

    return std::find(functions.begin(), functions.end(), name) != functions.end();
}

bool Token::isFunctionToken(const TokenType type) {
    return type == TokenType::COUNT || type == TokenType::SUM || type == TokenType::AVG ||
           type == TokenType::MAX || type == TokenType::MIN || type == TokenType::COALESCE ||
           type == TokenType::NOW || type == TokenType::CURRENT_DATE || type == TokenType::CAST;
}

bool Token::isAggregateFunction(const std::string& name) {
static const std::unordered_set<std::string> aggregates = {
    "COUNT", "SUM", "AVG", "MIN", "MAX"
};
return aggregates.count(name) > 0;
}


} // namespace sql_parser