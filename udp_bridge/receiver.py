#!/usr/bin/env python3
import termios
import sys
import tty
import rclpy
from rclpy.node import Node
import socket
import pickle
import base64
import select
import time
from threading import Thread
from udp_bridge.aes_helper import AESCipher


class UdpReceiver:
    def __init__(self, node:Node):
        self.node = node
        port = node.get_parameter("port").value
        node.get_logger().info("Initializing udp_bridge on port " + str(port))

        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        self.sock.settimeout(1)

        self.known_senders = []  # type: list

        self.cipher = AESCipher(node.get_parameter("encryption_key").value)

        self.publishers = {}

    def recv_message(self):
        """
        Receive a message from the network, process it and publish it into ROS
        """
        self.sock.settimeout(1)
        acc = bytes()
        while rclpy.ok():
            try:
                acc += self.sock.recv(10240)

                if acc[-3:] == b'\xff\xff\xff':  # our package delimiter
                    self.handle_message(acc[:-3])
                    acc = bytes()

            except socket.timeout:
                pass

    def handle_message(self, msg: bytes):
        """
        Handle a new message which came in from the socket
        """
        try:
            dec_msg = self.cipher.decrypt(msg)
            bin_msg = base64.b64decode(dec_msg)
            deserialized_msg = pickle.loads(bin_msg)

            data = deserialized_msg['data']
            topic = deserialized_msg['topic']
            hostname = deserialized_msg['hostname']

            if hostname not in self.known_senders:
                self.known_senders.append(hostname)

            self.publish(topic, data, hostname)
        except Exception as e:
            node.get_logger().error('Could not deserialize received message with error {}'.format(str(e)))

    def publish(self, topic: str, msg, hostname: str):
        """
        Publish a message into ROS

        :param topic: The topic on which the message was sent on the originating host
        :param msg: The ROS message which was sent on the originating host
        :param hostname: The hostname of the originating host
        """

        # publish msg under host namespace
        namespaced_topic = "{}/{}".format(hostname, topic).replace("//", "/")

        # create a publisher object if we don't have one already
        if namespaced_topic not in self.publishers.keys():
            node.get_logger().info('Publishing new topic {}'.format(namespaced_topic))
            self.publishers[namespaced_topic] = self.node.create_publisher(type(msg),namespaced_topic, 1)

        self.publishers[namespaced_topic].publish(msg)


def validate_params(node:Node) -> bool:
    result = True

    if not node.has_parameter("port"):
        node.get_logger().fatal("parameter 'port' not found")
        result = False
    if not isinstance(node.get_parameter("port").value, int):
        node.get_logger().fatal("parameter 'port' is not an Integer")
        result = False

    return result


def main():
    rclpy.init()
    node = Node("udp_bridge_receiver", automatically_declare_parameters_from_overrides=True)
    if validate_params(node):
        # setup udp receiver
        receiver = UdpReceiver(node)
        thread = Thread(target=rclpy.spin, args=(node,))
        thread.start()
        receiver.recv_message()
