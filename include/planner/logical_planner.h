#pragma once
#include "sql_parser/parser.h"
#include <unordered_set>

namespace logical_planner {
// Logical operator types
    enum class LogicalOperatorType {
        TABLE_SCAN,
        INDEX_SCAN,
        NESTED_LOOP_JOIN,
        HASH_JOIN,
        MERGE_JOIN,
        FILTER,
        PROJECT,
        AGGREGATE,
        SORT,
        LIMIT,
        UNION,
        INTERSECT,
        EXCEPT,
        SUBQUERY
    };

    // Join types
    enum class JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS,
        SEMI,
        ANTI
    };

    // Aggregate function types
    enum class AggregateType {
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        STDDEV,
        VARIANCE,
        COUNT_DISTINCT
    };

    // Column information
    struct ColumnInfo {
        std::string tableName;
        std::string columnName;
        std::string alias;
        std::string dataType;
        bool nullable = true;
        bool isPrimaryKey = false;
        bool hasIndex = false;

        [[nodiscard]] std::string getFullName() const {
            return tableName.empty() ? columnName : tableName + "." + columnName;
        }

        [[nodiscard]] std::string getDisplayName() const {
            return alias.empty() ? getFullName() : alias;
        }
    };

    // Table statistics for cost estimation
    struct TableStats {
        std::string tableName;
        size_t rowCount = 0;
        size_t avgRowSize = 0;
        std::unordered_map<std::string, size_t> columnCardinality;
        std::unordered_set<std::string> indexedColumns;

        [[nodiscard]] double getSelectivity(const std::string& column) const {
            if (const auto it = columnCardinality.find(column); it != columnCardinality.end() && rowCount > 0) {
                return static_cast<double>(it->second) / static_cast<double>(rowCount);
            }
            return 0.1; // Default selectivity
        }
    };
    // Base class for all logical operators
    class LogicalOperator {
    public:
        LogicalOperatorType type;
        std::vector<std::unique_ptr<LogicalOperator>> children;
        std::vector<ColumnInfo> outputSchema;
        double estimatedCost = 0.0;
        size_t estimatedRowCount = 0;

        explicit LogicalOperator(const LogicalOperatorType t) : type(t) {}
        virtual ~LogicalOperator() = default;

        [[nodiscard]] virtual std::string toString() const = 0;
        [[nodiscard]] virtual std::unique_ptr<LogicalOperator> clone() const = 0;
        virtual double calculateCost(const std::unordered_map<std::string, TableStats>& stats) = 0;

        void addChild(std::unique_ptr<LogicalOperator> child) {
            children.push_back(std::move(child));
        }

        [[nodiscard]] LogicalOperator* getChild(const size_t index) const {
            return index < children.size() ? children[index].get() : nullptr;
        }

        [[nodiscard]] size_t getChildCount() const {
            return children.size();
        }
    };

    // Table scan operator
    class TableScanOperator final : public LogicalOperator {
    public:
        std::string tableName;
        std::string alias;
        std::vector<std::string> projectedColumns;

        explicit TableScanOperator(std::string  table, std::string  tableAlias = "");
        ~TableScanOperator() override = default;
        [[nodiscard]] std::string toString() const override;
        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;
        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;
    };

    // Filter operator (WHERE conditions)
    class FilterOperator final : public LogicalOperator {
    public:
        std::string condition;
        std::unique_ptr<sql_parser::ASTNode> conditionAST;
        double selectivity = 0.1; // Default 10% selectivity

        explicit FilterOperator(std::string  cond);
        explicit FilterOperator(std::unique_ptr<sql_parser::ASTNode> condAST);
        ~FilterOperator() override = default;
        [[nodiscard]] std::string toString() const override;
        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;
        double calculateCost(const std::unordered_map<std::string, TableStats> &stats) override;
        void setSelectivity(double sel);
    };

    // Projection operator (SELECT columns)
    class ProjectionOperator final : public LogicalOperator {
    public:
        std::vector<std::string> projectedColumns;
        std::vector<std::string> expressions;

        explicit ProjectionOperator(const std::vector<std::string>& columns);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;

        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;
    };

    // Join operator
    class JoinOperator final : public LogicalOperator {
    public:
        JoinType joinType;
        std::string joinCondition;
        std::unique_ptr<sql_parser::ASTNode> conditionAST;
        std::vector<std::pair<std::string, std::string>> joinColumns; // left, right column pairs

        JoinOperator(JoinType type, std::string  condition);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;

        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;

        void optimizeJoinAlgorithm();
    };

    // Aggregate operator (GROUP BY, HAVING)
    class AggregateOperator final : public LogicalOperator {
    public:
        std::vector<std::string> groupByColumns;
        std::vector<std::pair<AggregateType, std::string>> aggregates; // function, column
        std::string havingCondition;

        explicit AggregateOperator(const std::vector<std::string>& groupBy);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;

        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;

    private:
        static std::string aggregateTypeToString(AggregateType type);
    };

    // Sort operator (ORDER BY)
    class SortOperator final : public LogicalOperator {
    public:
        std::vector<std::pair<std::string, bool>> sortColumns; // column, ascending

        explicit SortOperator(const std::vector<std::pair<std::string, bool>>& columns);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;

        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;
    };

    // Limit operator
    class LimitOperator final : public LogicalOperator {
    public:
        size_t limit;
        size_t offset;

        explicit LimitOperator(size_t lim, size_t off = 0);

        [[nodiscard]] std::string toString() const override;

        [[nodiscard]] std::unique_ptr<LogicalOperator> clone() const override;

        double calculateCost(const std::unordered_map<std::string, TableStats>& stats) override;
    };
} // namespace logical_planner