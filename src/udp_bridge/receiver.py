#!/usr/bin/env python3

import rospy
import rostopic
import socket
import pickle
import base64

from udp_bridge.aes_helper import AESCipher


class UdpReceiver:
    def __init__(self):
        port = rospy.get_param("udp_bridge/port")
        rospy.init_node("udp_bridge_receiver", log_level=rospy.INFO)
        rospy.loginfo("Initializing udp_bridge on port " + str(port))

        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))

        self.cipher = AESCipher(rospy.get_param("udp_bridge/encryption_key", None))

        self.publishers = {}
        self.recv_messages()

    def recv_messages(self):
        self.sock.settimeout(1)
        acc = bytes()
        while not rospy.is_shutdown():
            try:
                acc += self.sock.recv(10240)

                if acc[-3:] == b'\xff\xff\xff':  # our package delimiter
                    self.handle_message(acc[:-3])
                    acc = bytes()

            except socket.timeout:
                pass

    def handle_message(self, msg):
        """
        Handle a new message which came in from the socket
        :type msg bytes
        """
        try:
            dec_msg = self.cipher.decrypt(msg)
            bin_msg = base64.b64decode(dec_msg)
            deserialized_msg = pickle.loads(bin_msg)

            data = deserialized_msg['data']
            topic = deserialized_msg['topic']
            hostname = deserialized_msg['hostname']
            self.publish(topic, data, hostname)
        except Exception as e:
            rospy.logerr('Could not deserialize received message wither error {}'.format(str(e)))

    def publish(self, topic, msg, hostname):
        namespaced_topic = "{}/{}".format(hostname, topic)

        if topic not in self.publishers.keys():
            rospy.loginfo('Publishing new topic {}:{}'.format(hostname, topic))
            self.publishers[namespaced_topic] = rospy.Publisher(namespaced_topic, type(msg), tcp_nodelay=True, queue_size=5)

        self.publishers[namespaced_topic].publish(msg)


def validate_params():
    """:rtype: bool"""
    result = True
    if not rospy.has_param("udp_bridge"):
        rospy.logfatal("parameter 'udp_bridge' not found")
        result = False

    if not rospy.has_param("udp_bridge/port"):
        rospy.logfatal("parameter 'udp_bridge/port' not found")
        result = False
    if not isinstance(rospy.get_param("udp_bridge/port"), int):
        rospy.logfatal("parameter 'udp_bridge/port' is not an Integer")
        result = False

    return result


if __name__ == '__main__':
    if validate_params():
        receiver = UdpReceiver()

