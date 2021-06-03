#!/usr/bin/env python3
import rospy
import socket
from std_msgs import msg
from bitbots_test.test_case import RosNodeTestCase


class SenderTestCase(RosNodeTestCase):
    def test_topic_gets_published_and_sent(self):
        # setup makeshift receiver
        port = rospy.get_param("/udp_bridge/port")
        sock = socket.socket(type=socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", port))
        sock.settimeout(1.0)

        # publish a test message
        topic = rospy.get_param("/udp_bridge/topics")[0]
        publisher = rospy.Publisher(topic, msg.String, latch=True, queue_size=1)
        publisher.publish(msg.String("Hello World"))

        # assert that a message is sent by trying to receive it
        self.assertIsNotNone(sock.recv(10240))


if __name__ == "__main__":
    from bitbots_test import run_rostests
    run_rostests(SenderTestCase)
