cmake_minimum_required(VERSION 2.8.3)
project(udp_bridge)

find_package(catkin REQUIRED bitbots_docs bitbots_test)

catkin_python_setup()

enable_bitbots_docs()
enable_bitbots_tests()

catkin_package()
