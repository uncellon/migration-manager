cmake_minimum_required(VERSION 3.16)
project(migration-manager)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

file(GLOB CPPS *.cpp)

find_package(nlohmann_json REQUIRED)

add_executable(${PROJECT_NAME} ${CPPS})
target_link_libraries(${PROJECT_NAME} mysqlcppconn)

install(TARGETS ${PROJECT_NAME} DESTINATION bin)