#pragma once

#include <string>

namespace sql_parser {

    enum class TokenType {
        // Core SQL Keywords
        SELECT, FROM, WHERE, ORDER, BY, GROUP, HAVING, LIMIT, OFFSET,
        INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE,
        TABLE, VIEW, INDEX, DATABASE, SCHEMA, COLUMN, CONSTRAINT,
        PRIMARY, FOREIGN, KEY, UNIQUE, CHECK, DEFAULT, NULL_KW,

        // Join Keywords
        JOIN, LEFT, RIGHT, INNER, OUTER, FULL, CROSS, NATURAL,
        ON, USING,

        // Logical Operators
        AND, OR, NOT, XOR,

        // Comparison Operators
        EQUALS,          // =
        NOT_EQUALS,      // != or <>
        LESS_THAN,       // <
        GREATER_THAN,    // >
        LESS_EQUAL,      // <=
        GREATER_EQUAL,   // >=
        IS, IS_NOT,      // IS, IS NOT
        LIKE, NOT_LIKE,  // LIKE, NOT LIKE
        ILIKE,           // ILIKE (PostgreSQL case-insensitive)
        SIMILAR,         // SIMILAR TO (PostgreSQL)
        REGEXP,          // REGEXP, RLIKE (MySQL-style regex)
        MATCH,           // MATCH (SQLite FTS)

        // Set Operations
        UNION, UNION_ALL, INTERSECT, EXCEPT, MINUS,

        // Conditional Expressions
        CASE, WHEN, THEN, ELSE, END,
        NULLIF, GREATEST, LEAST,

        // Subquery Keywords
        EXISTS, NOT_EXISTS, IN, NOT_IN, ANY, SOME, // ALL,

        // Range/Pattern Matching
        BETWEEN, NOT_BETWEEN, ESCAPE,

        // Arithmetic Operators
        PLUS,            // +
        //MINUS,           // -
        MULTIPLY,        // * (also wildcard)
        DIVIDE,          // /
        MODULO,          // %
        POWER,           // ^ (PostgreSQL) or ** (some dialects)

        // String Operators
        CONCAT,          // || (string concatenation)

        // Bitwise Operators (PostgreSQL/DuckDB)
        BIT_AND,         // &
        BIT_OR,          // |
        BIT_XOR,         // #
        BIT_NOT,         // ~
        BIT_SHIFT_LEFT,  // <<
        BIT_SHIFT_RIGHT, // >>

        // JSON Operators (PostgreSQL/DuckDB)
        JSON_EXTRACT,         // ->
        JSON_EXTRACT_TEXT,    // ->>
        JSON_PATH,            // #>
        JSON_PATH_TEXT,       // #>>
        JSON_CONTAINS,        // @>
        JSON_CONTAINED,       // <@
        JSON_HAS_KEY,         // ?
        JSON_HAS_ANY_KEY,     // ?|
        JSON_HAS_ALL_KEYS,    // ?&

        // Array Operators (PostgreSQL/DuckDB)
        ARRAY_SUBSCRIPT,      // [index]
        ARRAY_SLICE,          // [start:end]
        ARRAY_CONCAT,         // ||
        ARRAY_OVERLAP,        // &&
        ARRAY_CONTAINS,       // @>
        ARRAY_CONTAINED,      // <@

        // Assignment Operators
        ASSIGN,          // := (PostgreSQL functions/procedures)

        // Aggregate Functions
        COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE,
        STRING_AGG, ARRAY_AGG, JSON_AGG, JSON_OBJECT_AGG,
        BOOL_AND, BOOL_OR, EVERY,
        PERCENTILE_CONT, PERCENTILE_DISC,

        // Window Functions
        OVER, PARTITION, ROWS, RANGE, UNBOUNDED, PRECEDING, FOLLOWING, CURRENT,
        ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST,
        LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE,

        // Mathematical Functions
        ABS, CEIL, CEILING, FLOOR, ROUND, TRUNC, //TRUNCATE,
        SQRT, POWER_FUNC, EXP, LN, LOG, LOG10,
        SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2,
        DEGREES, RADIANS, PI,
        RANDOM, SETSEED,

        // String Functions
        LENGTH, CHAR_LENGTH, CHARACTER_LENGTH, OCTET_LENGTH,
        UPPER, LOWER, INITCAP, TITLE,
        LTRIM, RTRIM, TRIM, BTRIM,
        SUBSTRING, SUBSTR, LEFT_FUNC, RIGHT_FUNC,
        POSITION, STRPOS, INSTR,
        REPLACE, TRANSLATE, OVERLAY,
        CONCAT_FUNC, CONCAT_WS,
        SPLIT_PART, STRING_TO_ARRAY, ARRAY_TO_STRING,
        ASCII, CHR, MD5, SHA1, SHA256,
        ENCODE, DECODE, TO_HEX,

        // Date/Time Functions
        NOW, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP,
        LOCALTIME, LOCALTIMESTAMP,
        DATE_TRUNC, DATE_PART, EXTRACT,
        AGE, JUSTIFY_DAYS, JUSTIFY_HOURS, JUSTIFY_INTERVAL,
        MAKE_DATE, MAKE_TIME, MAKE_TIMESTAMP, MAKE_TIMESTAMPTZ,
        TO_DATE, TO_TIMESTAMP, TO_CHAR,
        CLOCK_TIMESTAMP, STATEMENT_TIMESTAMP, TRANSACTION_TIMESTAMP,

        // Type Conversion Functions
        CAST, CONVERT, TO_NUMBER, TO_CHAR_FUNC,

        // Null Handling Functions
        COALESCE, /*NULLIF,*/ ISNULL, IFNULL, NVL, NVL2,

        // Data Types
        INTEGER, INT, BIGINT, SMALLINT, TINYINT,
        DECIMAL, NUMERIC, REAL, FLOAT, DOUBLE, PRECISION,
        CHAR, VARCHAR, TEXT, CLOB,
        BINARY, VARBINARY, BLOB,
        BOOLEAN, BOOL,
        DATE, TIME, TIMESTAMP, TIMESTAMPTZ, TIMETZ, INTERVAL,
        UUID, JSON, JSONB, XML,
        ARRAY, ENUM,

        // PostgreSQL Specific Types
        SERIAL, BIGSERIAL, SMALLSERIAL,
        INET, CIDR, MACADDR, MACADDR8,
        POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE,
        TSQUERY, TSVECTOR,
        INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE,
        MONEY, OID,

        // DuckDB Specific Types
        HUGEINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT,
        MAP, STRUCT, LIST,

        // SQLite Specific (mostly the same as standard but some differences)
        // SQLite is dynamically typed, so fewer specific types

        // Constraints and Modifiers
        NOT_NULL, AUTO_INCREMENT, AUTOINCREMENT, IDENTITY,
        REFERENCES, DEFERRABLE, INITIALLY, DEFERRED, IMMEDIATE,
        MATCH_FULL, MATCH_PARTIAL, MATCH_SIMPLE,
        CASCADE, RESTRICT, SET_NULL, SET_DEFAULT, NO_ACTION,

        // Transaction Keywords
        BEGIN, START, TRANSACTION, WORK, COMMIT, ROLLBACK,
        SAVEPOINT, RELEASE, ISOLATION, LEVEL,
        READ, WRITE, ONLY, UNCOMMITTED, COMMITTED, REPEATABLE, SERIALIZABLE,

        // Index Keywords
        BTREE, HASH, GIST, SPGIST, GIN, BRIN, // PostgreSQL index types
        CONCURRENTLY, IF_NOT_EXISTS, IF_EXISTS,

        // Window Clause Keywords
        EXCLUDE, TIES, NO_OTHERS, GROUPS,

        // Set Operations Modifiers
        DISTINCT, ALL,

        // Common Table Expressions
        WITH, RECURSIVE, AS,

        // Conditional Keywords
        CASE_WHEN, CASE_ELSE, CASE_END,

        // Sorting Keywords
        ASC, DESC, NULLS, FIRST, LAST,

        // Function Keywords
        FUNCTION, PROCEDURE, RETURNS, RETURN, LANGUAGE,
        IMMUTABLE, STABLE, VOLATILE, STRICT, CALLED, INPUT, SECURITY,
        DEFINER, INVOKER, PARALLEL, SAFE, RESTRICTED, UNSAFE,

        // Administrative Keywords
        ANALYZE, VACUUM, REINDEX, CLUSTER, EXPLAIN, DESCRIBE, DESC_KW,
        GRANT, REVOKE, ROLE, USER, GROUP_ROLE,
        USAGE, EXECUTE, CONNECT, TEMPORARY, TEMP,

        // Special Literals and Identifiers
        TRUE_LIT, FALSE_LIT, NULL_LIT,
        CURRENT_USER, SESSION_USER, //USER,

        // Wildcard and Special Operators
        ASTERISK,        // * (multiplication OR wildcard - context dependent!)
        QUESTION,        // ? (parameter placeholder)
        DOLLAR,          // $ (parameter like $1, $2)
        AT_SIGN,         // @ (used in some SQL dialects)

        // Delimiters and Punctuation
        LEFT_PAREN,      // (
        RIGHT_PAREN,     // )
        LEFT_BRACKET,    // [
        RIGHT_BRACKET,   // ]
        LEFT_BRACE,      // {
        RIGHT_BRACE,     // }
        COMMA,           // ,
        SEMICOLON,       // ;
        DOT,             // .
        DOUBLE_DOT,      // .. (range operator in some contexts)
        COLON,           // :
        DOUBLE_COLON,    // :: (PostgreSQL type cast)

        // Quote Types
        SINGLE_QUOTE,    // '
        DOUBLE_QUOTE,    // "
        BACKTICK,        // ` (MySQL-style identifier quotes)
        DOLLAR_QUOTE,    // $$ (PostgreSQL dollar quoting)

        // Comments
        LINE_COMMENT,    // -- comment
        BLOCK_COMMENT,   // /* comment */

        // Literals
        STRING_LITERAL,
        NUMBER_LITERAL,
        INTEGER_LITERAL,
        FLOAT_LITERAL,
        BINARY_LITERAL,     // b'101010'
        HEX_LITERAL,        // x'DEADBEEF'
        UNICODE_LITERAL,    // u&'unicode string'
        IDENTIFIER,
        QUOTED_IDENTIFIER,  // "identifier"

        // Special Tokens
        WHITESPACE,
        NEWLINE,
        EOF_TOKEN,
        UNKNOWN,

        // Error Recovery Tokens
        UNEXPECTED_CHAR,
        UNTERMINATED_STRING,
        UNTERMINATED_COMMENT,
        INVALID_NUMBER,

        // Preprocessor-like (for some SQL dialects)
        INCLUDE,
        DEFINE,
        UNDEF,

        // Advanced PostgreSQL Features
        VARIADIC, INOUT, OUT_PARAM,
        TRIGGER, BEFORE, AFTER, INSTEAD, OF, FOR, EACH, ROW, STATEMENT,
        OLD, NEW, REFERENCING,
        WHEN_CLAUSE,

        // DuckDB Specific Extensions
        PRAGMA, INSTALL, LOAD, // DuckDB extensions
        FROM_CSV, FROM_PARQUET, FROM_JSON, // DuckDB FROM functions
        COPY_TO, COPY_FROM,

        // SQLite Specific Features
        ATTACH, DETACH, // SQLite database attachment
        WITHOUT_ROWID,  // SQLite table option
        VIRTUAL,        // Virtual tables

        // Advanced SQL Features (SQL:2016 and beyond)
        LATERAL, // Lateral joins
        GROUPING, ROLLUP, CUBE, GROUPING_SETS, // OLAP extensions
        FILTER, // Aggregate filters: COUNT(*) FILTER (WHERE ...)

        // Regular Expression Operators (PostgreSQL)
        REGEX_MATCH,        // ~
        REGEX_NOT_MATCH,    // !~
        REGEX_MATCH_CI,     // ~*
        REGEX_NOT_MATCH_CI,  // !~*

        //Extra
        TRUE_LITERAL, FALSE_LITERAL, IS_NOT_NULL, IS_NULL, NULL_LITERAL, DAY, HOUR, MINUTE, SECOND, MONTH, YEAR
    };

    struct Token {
        TokenType type;
        std::string value;
        size_t position;

        Token(TokenType t, std::string v, size_t pos);

        static std::string tokenTypeToString(TokenType type);

        static bool isKeyword(TokenType type);

        static bool isOperator(TokenType type);

        static bool isLiteral(TokenType type);

        static bool isReservedKeyword(const std::string &word);

        static bool isFunction(const std::string &name);

        static bool isFunctionToken(TokenType type);

        static bool isAggregateFunction(const std::string &name);
    };

} // namespace sql_parser