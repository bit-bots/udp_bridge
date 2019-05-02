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
                acc += self.sock.recv(2048)
                if acc[-1] == 10:   # 10 is \n in a bytestring
                    self.handle_message(acc[:-1])
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
            self.publish(topic, data)
        except Exception as e:
            rospy.logerr(0.5, 'Could not deserialize received message wither error {}'.format(type(e)))

    def publish(self, topic, msg):
        if topic not in self.publishers.keys():
            self.publishers[topic] = rospy.Publisher(topic, type(msg), tcp_nodelay=True, queue_size=5)

        self.publishers[topic].publish(msg)


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

