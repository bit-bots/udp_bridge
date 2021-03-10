#!/usr/bin/env python3
import time
import unittest
from unittest.mock import Mock
import rostest
import rospy
from socket import gethostname
from std_msgs import msg


class SenderReceiverTestCase(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        rospy.init_node(type(self).__name__, anonymous=True)

    def test_sent_message_gets_received_over_bridge(self):
        send_topic = rospy.get_param("/udp_bridge/topics")[0]
        receive_topic = f"{gethostname()}/{send_topic}".replace("//", "/")

        rospy.logwarn(f"sending on {send_topic}, receiving on {receive_topic}")

        # setup a subscriber on where the message will be received
        mock_on_msg = Mock()
        subscriber = rospy.Subscriber(receive_topic, msg.String, mock_on_msg)

        # publish a test message
        publisher = rospy.Publisher(send_topic, msg.String, latch=True, queue_size=1)
        publisher.publish(msg.String("Hello World"))

        time.sleep(0.5)
        mock_on_msg.assert_called_once()


if __name__ == "__main__":
    rostest.rosrun("udp_bridge", SenderReceiverTestCase.__name__, SenderReceiverTestCase)
