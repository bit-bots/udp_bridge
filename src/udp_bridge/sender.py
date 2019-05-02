#!/usr/bin/env python3

import rospy
import rostopic
import socket
import pickle
import base64

from udp_bridge.aes_helper import AESCipher


class UdpSender:
    def __init__(self, target_ip, port):
        rospy.init_node("udp_bridge_sender", anonymous=True, log_level=rospy.INFO)
        rospy.loginfo("Initializing udp_bridge to '" + target_ip + ":" + str(port) + "'")

        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.target = (target_ip, port)

        self.cipher = AESCipher(rospy.get_param("udp_bridge/encryption_key", None))

        self.subscribers = []
        for topic in rospy.get_param("udp_bridge/topics"):
            self.setup_topic_subscriber(topic)

    def setup_topic_subscriber(self, topic):
        """Try to setup a subscriber for the specified topic until it works"""
        data_class, _, _ = rostopic.get_topic_class(topic)
        if data_class is not None:
            self.subscribers.append(
                rospy.Subscriber(topic, data_class, self.topic_callback, topic, queue_size=2, tcp_nodelay=True)
            )
            rospy.loginfo("Subscribed to topic " + topic)
        else:
            backoff = 1
            rospy.logwarn("Topic " + topic + " is not yet known. Retrying in " + str(backoff) + " seconds")
            rospy.Timer(rospy.Duration(backoff), lambda event: self.setup_topic_subscriber(topic), oneshot=True)

    def topic_callback(self, data, topic):
        serialized_data = base64.b64encode(pickle.dumps({
            "data": data,
            "topic": topic
        }, pickle.HIGHEST_PROTOCOL)).decode("ASCII")
        enc_data = self.cipher.encrypt(serialized_data)
        try:
            self.sock.sendto(enc_data + b'\n', self.target)
        except Exception as e:
            rospy.logerr_throttle(0.5, 'Could not send data from topic {} to {}: {}'.format(topic, self.target, type(e)))


def validate_params():
    """:rtype: bool"""
    result = True
    if not rospy.has_param("udp_bridge"):
        rospy.logfatal("parameter 'udp_bridge' not found")
        result = False

    if not rospy.has_param("udp_bridge/target_ips"):
        rospy.logfatal("parameter 'udp_bridge/target_ips' not found")
        result = False
    target_ips = rospy.get_param("udp_bridge/target_ips")
    if not isinstance(target_ips, list):
        rospy.logfatal("parameter udp_bridge/target_ips is not a list")
        result = False
    for addr in target_ips:
        try:
            socket.inet_aton(addr)
        except Exception as e:
            rospy.logfatal("Cannot parse " + str(addr) + " as IP Address")
            result = False

    if not rospy.has_param("udp_bridge/port"):
        rospy.logfatal("parameter 'udp_bridge/port' not found")
        result = False
    if not isinstance(rospy.get_param("udp_bridge/port"), int):
        rospy.logfatal("parameter 'udp_bridge/port' is not an Integer")
        result = False

    if not rospy.has_param("udp_bridge/topics"):
        rospy.logfatal("parameter 'udp_bridge/port' not found")
        result = False
    if not isinstance(rospy.get_param("udp_bridge/topics"), list):
        rospy.logfatal("parameter 'udp_bridge/topics' is not a list")
        result = False
    if len(rospy.get_param("udp_bridge/topics")) == 0:
        rospy.logwarn("parameter 'udp_bridge/topics' is an empty list")

    return result


if __name__ == '__main__':
    if validate_params():
        port = rospy.get_param("udp_bridge/port")
        senders = []
        for ip in rospy.get_param("udp_bridge/target_ips"):
            senders.append(UdpSender(ip, port))

        rospy.spin()

