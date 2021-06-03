cmake_minimum_required(VERSION 2.8.3)
project(udp_bridge)

find_package(catkin REQUIRED bitbots_docs)

catkin_python_setup()

enable_bitbots_docs()

catkin_package()

if (CATKIN_ENABLE_TESTING)
    find_package(catkin REQUIRED COMPONENTS bitbots_test)
    enable_bitbots_tests()
endif()
