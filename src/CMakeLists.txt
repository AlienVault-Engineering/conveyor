CMAKE_MINIMUM_REQUIRED(VERSION 2.7)

set (PROJECT_NAME conveyor)
PROJECT(${PROJECT_NAME})

file(GLOB HDRS "../include/*.h*" "*.h*")
file(GLOB HDRS "*.c*")

include_directories(../include )
include_directories(.. $ENV{DEPDIR}/include )

add_library (${PROJECT_NAME} ${SRCS} ${HDRS})

install(TARGETS ${PROJECT_NAME} ARCHIVE DESTINATION lib)
