CMAKE_MINIMUM_REQUIRED(VERSION 2.7)

set (PROJECT_NAME conveyor-project)
PROJECT(${PROJECT_NAME})

set(CMAKE_SUPPRESS_REGENERATION true)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF) # want c++11 not gnu++11

set(CMAKE_CXX_RELEASE_FLAGS "-DNDEBUG=1")

set(DEPDIR $ENV{DEPDIR})

# find dependencies
FIND_LIBRARY(GTEST_LIB gtest HINT ${DEPDIR}/lib )

add_subdirectory(src)

if ("${GTEST_LIB}" STREQUAL "GTEST_LIB-NOTFOUND")
else()
  add_subdirectory(tests)
endif()

install(DIRECTORY include DESTINATION . FILES_MATCHING PATTERN "*.h" )
