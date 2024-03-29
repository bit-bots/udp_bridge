#!/usr/bin/env python3
from socket import gethostname

import rospy
from bitbots_test.mocks import MockSubscriber
from bitbots_test.test_case import RosNodeTestCase
from std_msgs import msg


class SenderReceiverTestCase(RosNodeTestCase):
    def test_sent_message_gets_received_over_bridge(self):
        # setup
        send_topic = rospy.get_param("/udp_bridge/topics")[0]
        receive_topic = f"{gethostname()}/{send_topic}".replace("//", "/")
        subscriber = MockSubscriber(receive_topic, msg.String)

        # execution
        publisher = rospy.Publisher(send_topic, msg.String, latch=True, queue_size=1)
        publisher.publish(msg.String("Hello World"))

        # verification
        self.with_assertion_grace_period(subscriber.assertOneMessageReceived, 1000 * 5)


if __name__ == "__main__":
    from bitbots_test import run_rostests

    run_rostests(SenderReceiverTestCase)
