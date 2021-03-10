cmake_minimum_required(VERSION 2.8.3)
project(udp_bridge)

find_package(catkin REQUIRED bitbots_docs)

catkin_python_setup()

if (CATKIN_ENABLE_TESTING)
    find_package(rostest REQUIRED)
    find_package(rosunit REQUIRED)
    catkin_add_nosetests(test/unit_tests)
    add_rostest(test/integration_tests/test_sender.launch)
    add_rostest(test/integration_tests/test_sender_receiver.launch)
endif()

enable_bitbots_docs()

catkin_package()
