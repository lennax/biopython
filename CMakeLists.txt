cmake_minimum_required(VERSION 2.8)

project(biopython-lennax NONE)

set(Python_ADDITIONAL_VERSIONS 2.6)
find_package(PythonInterp 2.6 REQUIRED)
enable_testing()

file(GLOB_RECURSE pythonSources "${CMAKE_SOURCE_DIR}/*.py")

add_custom_target(build ALL
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} 
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/setup.py build
    SOURCES ${pythonSources}
    )

#add_test(
    #NAME test_all 
    #WORKING_DIRECTORY ${CMAKE_SOURCE_DIR} 
    #COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/setup.py test
    #)

add_test(
    NAME test_cif
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/Tests
    COMMAND ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/Tests/run_tests.py test_MMCIF
    )
