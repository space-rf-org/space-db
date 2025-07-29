#include <iostream>
#include "sql_parser/lexer.h"
#include "sql_parser/parser.h"
#include "logical_planner.cpp"
#include "planner/physical_planner.h"
#include "planner/plan_analyzer.h"
//#include "planner/execution_framework.h"

int main() {
    std::string sql = R"(
        SELECT
            u.id,
            u.name,
            (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id AND o.created_at > NOW() - INTERVAL '30 days') AS recent_order_count,
            CASE
                WHEN u.age < 18 THEN 'minor'
                WHEN u.age BETWEEN 18 AND 65 THEN 'adult'
                ELSE 'senior'
            END AS age_group,
            COALESCE(p.phone, 'N/A') AS phone_number
        FROM users u
        LEFT JOIN profiles p ON p.user_id = u.id
        WHERE
            u.status = 'active'
            AND (
                EXISTS (
                    SELECT 1
                    FROM logins l
                    WHERE l.user_id = u.id AND l.timestamp > CURRENT_DATE - INTERVAL '7 days'
                )
                OR u.created_at > CURRENT_DATE - INTERVAL '14 days'
            )
        GROUP BY u.id, u.name, p.phone, u.age
        HAVING COUNT(*) > 1
        ORDER BY recent_order_count DESC, u.name ASC
        LIMIT 50 OFFSET 10;
    )";

    try {
        // Tokenize
        sql_parser::SQLLexer lexer(sql);
        auto tokens = lexer.tokenize();

        std::cout << "=== TOKENS ===" << std::endl;
        for (const auto& token : tokens) {
            if (token.type != sql_parser::TokenType::EOF_TOKEN) {
                //std::cout << "Type: " << static_cast<int>(token.type)
                //         << ", Value: '" << token.value << "'" << std::endl;
            }
        }

        // Parse
        sql_parser::SQLParser parser(std::move(tokens));
        auto ast = parser.parse();

        std::cout << "\n=== PARSED AST ===" << std::endl;
        std::cout << ast->toString() << std::endl;

        const auto planner = logical_planner::createSamplePlanner();
        const auto logicalPlan = planner->createPlan(dynamic_cast<const sql_parser::SelectStatement &>(*ast));
        std::cout << "\n=== PLAN ===" << std::endl;
        std::cout << "Logical Plan:\n" << planner->printPlan(logicalPlan.get());

        auto context = physical_planner::ExecutionContext();
        // Create planner with system configuration
        const auto p_planner = new physical_planner::PhysicalPlanner(context);

        // Convert logical plan to physical plan
        auto physicalPlan = p_planner->createPhysicalPlan(logicalPlan.get());

        // Optimize the physical plan
        physicalPlan = p_planner->optimizePhysicalPlan(std::move(physicalPlan));

        // Create analyzer and simulator
        physical_planner::PlanAnalyzer analyzer(context);
        physical_planner::ExecutionSimulator simulator(context);

        // Analyze the plan
        auto analysis = analyzer.analyzePlan(physicalPlan.get());
        std::cout << analyzer.generatePlanReport(analysis) << std::endl;

        // Simulate execution
        auto simulation = simulator.simulateExecution(physicalPlan.get());
        std::cout << simulator.generateSimulationReport(simulation) << std::endl;

        // Test under memory pressure
        auto memoryPressureTest = simulator.simulateWithMemoryPressure(physicalPlan.get(), 0.5);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }


    return 0;
}
