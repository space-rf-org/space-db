cmake_minimum_required(VERSION 3.15)
project(space_db VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Add compile options
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

# Include directories
include_directories(include)

# Create library
add_library(logical_planner_lib
        src/token.cpp
        src/ast_nodes.cpp
        src/lexer.cpp
        src/parser.cpp
        src/logical_planner.cpp
        src/physical_planner.cpp
)

target_include_directories(logical_planner_lib PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Create main executable
add_executable(logical_planner src/main.cpp)
target_link_libraries(logical_planner logical_planner_lib)

# Enable testing
enable_testing()
#add_subdirectory(tests)

# Add examples
#add_subdirectory(examples)

# Install targets
install(TARGETS logical_planner logical_planner_lib
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)