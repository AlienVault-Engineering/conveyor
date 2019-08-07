CMAKE_MINIMUM_REQUIRED(VERSION 2.7)

set (PROJECT_NAME conveyor-tests)
PROJECT(${PROJECT_NAME})

file(GLOB HDRS "../include/*.h*" "*.h*")
file(GLOB SRCS "*.cpp")

include_directories(../include )
include_directories(.. $ENV{DEPDIR}/include )
include_directories(.. $ENV{GTESTDIR}/include )

set(CMAKE_EXE_LINKER_FLAGS "-L$ENV{DEPDIR}/lib")

add_executable (${PROJECT_NAME} ${SRCS} ${HDRS})

TARGET_LINK_LIBRARIES(${PROJECT_NAME} conveyor )

target_link_libraries (${PROJECT_NAME} gtest
    boost_filesystem boost_system
    )
