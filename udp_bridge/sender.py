#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
import socket
import pickle
import base64
from threading import Lock, Thread
from queue import Queue, Empty, Full

from udp_bridge.aes_helper import AESCipher


class AutoSubscriber:
    """
    A class which automatically subscribes to a topic as soon as it becomes available and buffers received messages
    in a queue.
    """

    def __init__(self, topic, queue_size, node:Node):
        """
        :param topic: Topic to subscribe to
        :type topic: str
        :param queue_size: How many received messages should be buffered
        :type queue_size: int
        """
        self.topic = topic
        self.queue = Queue(queue_size)
        self.node = node

        self.__subscriber = None        # type: rospy.Subscriber
        self.__subscribe()

    def __subscribe(self, backoff=1.0):
        """
        Try to subscribe to the set topic
        :param backoff: How long to wait until another try
        """
        data_class, _, _ = rostopic.get_topic_class(self.topic)
        if data_class is not None:
            # topic is known
            self.__subscriber = self.node.create_subscription(data_class, self.topic, self.__message_callback, 1)
            self.node.get_logger().info('Subscribed to topic {}'.format(self.topic))

        else:
            # topic is not yet known
            self.node.get_logger().info('Topic {} is not yet known. Retrying in {} seconds'.format(self.topic, int(backoff)))
            if backoff > 30:
                backoff = 30
            self.timer = self.node.create_timer(
                rclpy.Duration(seconds=int(backoff)),
                lambda event: self.__subscribe(backoff * 1.2),
                oneshot=True
            )

    def __message_callback(self, data):
        serialized_data = base64.b64encode(pickle.dumps({
            "data": data,
            "topic": self.topic,
            "hostname": hostname
        }, pickle.HIGHEST_PROTOCOL)).decode("ASCII")
        enc_data = cipher.encrypt(serialized_data)

        try:
            self.queue.put(enc_data, block=True, timeout=0.5)
        except Full:
            self.node.get_logger().warn('Could enqueue new message of topic {}. Queue full.'.format(self.topic))


def validate_params(node:Node):
    """:rtype: bool"""
    result = True
    if not node.has_parameter("target_ips"):
        node.get_logger().fatal("parameter 'target_ips' not found")
        result = False
    target_ips = node.get_parameter("target_ips").value
    if not isinstance(target_ips, list):
        node.get_logger().fatal("parameter target_ips is not a list")
        result = False
    for addr in target_ips:
        try:
            socket.inet_aton(addr)
        except Exception as e:
            node.get_logger().fatal("Cannot parse " + str(addr) + " as IP Address")
            result = False

    if not node.has_parameter("port"):
        node.get_logger().fatal("parameter 'port' not found")
        result = False
    if not isinstance(node.get_parameter("port").value, int):
        node.get_logger().fatal("parameter 'port' is not an Integer")
        result = False

    if not node.has_parameter("topics"):
        node.get_logger().fatal("parameter 'port' not found")
        result = False
    if not isinstance(node.get_parameter("topics").value, list):
        node.get_logger().fatal("parameter 'topics' is not a list")
        result = False
    if len(node.get_parameter("topics").value) == 0:
        node.get_logger().warn("parameter 'topics' is an empty list")

    if not node.has_parameter('sender_queue_max_size'):
        node.get_logger().fatal("parameter 'sender_queue_max_size' not found")
        result = False
    if not isinstance(node.get_parameter('sender_queue_max_size').value, int):
        node.get_logger().fatal("parameter 'sender_queue_max_size' is not an Integer")
        result = False

    if not node.has_parameter('send_frequency'):
        node.get_logger().fatal("parameter 'send_frequency' not found")
        result = False
    if not isinstance(node.get_parameter('send_frequency').value, float) \
        and not isinstance(node.get_parameter('send_frequency').value, int):
        node.get_logger().fatal("parameter 'send_frequency' is not an Integer or Float")
        result = False

    return result


def main():
    rclpy.init()
    node = Node('udp_bridge_sender', automatically_declare_parameters_from_overrides=True)
    thread = Thread(target = rclpy.spin, args = (node))
    thread.start()
    if validate_params(node):
        hostname = socket.gethostname()
        encryption_key = None
        if node.has_parameter("encryption_key"):
            node.get_parameter("encryption_key").value
        cipher = AESCipher(encryption_key)
        port = node.get_parameter("port").value
        freq = node.get_parameter("send_frequency").value
        targets = node.get_parameter('target_ips').value
        max_queue_size = node.get_parameter('sender_queue_max_size').value
        topics = node.get_parameter("topics").value

        sock = socket.socket(type=socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        subscribers = []

        for topic in topics:
            subscribers.append(AutoSubscriber(topic, max_queue_size, node))

        while rclpy.ok:
            for subscriber in subscribers:
                try:
                    data = subscriber.queue.get_nowait()

                    for target in targets:
                        try:
                            sock.sendto(data + b'\xff\xff\xff', (target, port))
                        except Exception as e:
                            node.get_logger().error('Could not send data of topic {} to {} with error {}'
                                            .format(subscriber.topic, target, str(e)))

                except Empty:
                    pass

            node.get_clock().sleep_for(Duration(seconds=0, nanoseconds=int(1000000000 / freq)))
