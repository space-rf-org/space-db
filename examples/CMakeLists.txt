add_executable(example_usage example_usage.cpp)
target_link_libraries(example_usage sql_parser_lib)

# Copy SQL file to build directory
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/complex_query.sql
        ${CMAKE_CURRENT_BINARY_DIR}/complex_query.sql COPYONLY)