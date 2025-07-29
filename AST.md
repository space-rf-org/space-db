Expression Types:

UnaryExpression - For NOT, -, + operators
WindowFunction - OVER clauses with PARTITION BY, ORDER BY, and frame specifications
CastExpression - CAST(expr AS type) and expr::type syntax
InExpression - IN and NOT IN with value lists or subqueries
ExistsExpression - EXISTS and NOT EXISTS
BetweenExpression - BETWEEN and NOT BETWEEN
LikeExpression - LIKE, NOT LIKE, and PostgreSQL's ILIKE

Table and Join Enhancements:

TableReference - Enhanced to handle schema.table and multiple joins
Enhanced JoinClause - Added FULL OUTER, CROSS, NATURAL joins and USING clauses

Query Structure:

CTEDefinition and WithClause - Common Table Expressions (WITH clause)
UnionStatement - UNION, UNION ALL, INTERSECT, EXCEPT operations
Enhanced SelectStatement with WITH clause and DISTINCT support

DML Statements:

InsertStatement - INSERT with ON CONFLICT support
UpdateStatement - UPDATE with FROM clause support (PostgreSQL)
DeleteStatement - Basic DELETE statement

DDL Statements:

CreateTableStatement - CREATE TABLE with column definitions and constraints
DropTableStatement - DROP TABLE with CASCADE/RESTRICT
CreateIndexStatement - CREATE INDEX with partial index support

Database-Specific Features:

ArrayExpression and ArrayAccess - PostgreSQL arrays
JsonExpression - JSON operators for PostgreSQL and SQLite JSON1
ExplainStatement - EXPLAIN with various options

Enhanced Base Types:

Added bool and nullptr_t to Literal for TRUE/FALSE/NULL
Added distinct flag to FunctionCall for COUNT(DISTINCT x)

This should cover the major SQL constructs needed for PostgreSQL, SQLite, and DuckDB compatibility. The AST is now comprehensive enough to handle most real-world SQL queries across these databases.
