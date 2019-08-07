CMAKE_MINIMUM_REQUIRED(VERSION 2.7)

set (PROJECT_NAME conveyor-project)
PROJECT(${PROJECT_NAME})

set(CMAKE_SUPPRESS_REGENERATION true)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF) # want c++11 not gnu++11

set(CMAKE_CXX_RELEASE_FLAGS "-DNDEBUG=1")

set(DEPDIR $ENV{DEPDIR})

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

if(WINDOWS)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT /NODEFAULTLIB")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
  add_definitions(-DBOOST_ALL_NO_LIB)
endif()

find_package(Boost REQUIRED COMPONENTS filesystem system)
include_directories(${Boost_INCLUDE_DIRS})
link_libraries(${Boost_LIBRARIES})


# find dependencies
FIND_LIBRARY(GTEST_LIB gtest HINT ${DEPDIR}/lib )

add_subdirectory(src)

if ("${GTEST_LIB}" STREQUAL "GTEST_LIB-NOTFOUND")
else()
  add_subdirectory(tests)
endif()

install(DIRECTORY include DESTINATION . FILES_MATCHING PATTERN "*.h" )
