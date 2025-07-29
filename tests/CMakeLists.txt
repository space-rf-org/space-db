# Find or download a testing framework (e.g., Google Test)
find_package(GTest QUIET)

if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/03597a01ee50ed33e9fd7188b4304c7b0a8e4753.zip
    )
    FetchContent_MakeAvailable(googletest)
endif()

# Create test executable
add_executable(sql_parser_tests
        test_main.cpp
        test_lexer.cpp
        test_parser.cpp
)

target_link_libraries(sql_parser_tests
        sql_parser_lib
        gtest
        gtest_main
)

# Add tests
add_test(NAME sql_parser_tests COMMAND sql_parser_tests)