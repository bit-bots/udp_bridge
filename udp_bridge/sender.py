#!/usr/bin/env python3

from queue import Empty, Full, Queue
import socket
from threading import Thread
from typing import Optional

import rclpy
from rclpy.duration import Duration
from rclpy.node import Node
from rclpy.subscription import Subscription
from rclpy.timer import Timer
from ros2topic.api import get_msg_class, get_topic_names
from udp_bridge.message_handler import MessageHandler

HOSTNAME = socket.gethostname()


class AutoSubscriber:
    """
    A class which automatically subscribes to a topic as soon as it becomes available and buffers received messages
    in a queue.
    """

    def __init__(self, topic: str, queue_size: int, message_handler: MessageHandler, node: Node):
        """
        :param topic: Topic to subscribe to
        :type topic: str
        :param queue_size: How many received messages should be buffered
        :type queue_size: int
        """
        self.topic: str = topic
        self.queue: Queue = Queue(queue_size)
        self.message_handler: MessageHandler = message_handler
        self.node: Node = node
        self.timer: Optional[Timer] = None

        self.__subscriber: Optional[Subscription] = None
        self.__subscribe()

    def __subscribe(self, backoff=1.0):
        """
        Try to subscribe to the set topic
        :param backoff: How long to wait until another try (capped at 30s)
        """
        if backoff > 30:
            backoff = 30

        if self.timer:
            self.timer.cancel()

        data_class = None
        topics = get_topic_names(node=self.node)
        topic = next(filter(lambda t: t == self.topic, topics), None)

        if topic is not None:
            data_class = get_msg_class(self.node, topic)
            self.node.get_logger().info(str(data_class))

        if data_class is not None:
            # topic is known
            self.node.get_logger().info(f"Want to subscribe to topic {self.topic}")
            self.__subscriber = self.node.create_subscription(data_class, self.topic, self.__message_callback, 1)
            self.node.get_logger().info(f"Subscribed to topic {self.topic}")
        else:
            # topic is not yet known
            self.node.get_logger().info(f"Topic {self.topic} is not yet known. Retrying in {backoff} seconds")
            self.timer = self.node.create_timer(backoff, lambda: self.__subscribe(backoff * 1.2))

    def __message_callback(self, data):
        encrypted_msg = self.message_handler.encrypt_and_encode(
            {
                "data": data,
                "topic": self.topic,
                "hostname": HOSTNAME,
            }
        )

        try:
            self.queue.put(encrypted_msg, block=True, timeout=0.5)
        except Full:
            self.node.get_logger().warn(f"Could enqueue new message of topic {self.topic}. Queue full.")


# @TODO: replace by usage of https://github.com/PickNikRobotics/generate_parameter_library
def validate_params(node: Node) -> bool:
    result = True
    if not node.has_parameter("target_ips"):
        node.get_logger().fatal("parameter 'target_ips' not found")
        result = False
    target_ips = node.get_parameter("target_ips").value
    if not isinstance(target_ips, list):
        node.get_logger().fatal("parameter target_ips is not a list")
        result = False
    else:
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

    if not node.has_parameter("sender_queue_max_size"):
        node.get_logger().fatal("parameter 'sender_queue_max_size' not found")
        result = False
    if not isinstance(node.get_parameter("sender_queue_max_size").value, int):
        node.get_logger().fatal("parameter 'sender_queue_max_size' is not an Integer")
        result = False

    if not node.has_parameter("send_frequency"):
        node.get_logger().fatal("parameter 'send_frequency' not found")
        result = False
    if not isinstance(node.get_parameter("send_frequency").value, float) and not isinstance(
        node.get_parameter("send_frequency").value, int
    ):
        node.get_logger().fatal("parameter 'send_frequency' is not an Integer or Float")
        result = False

    return result


def setup_message_handler(node: Node) -> MessageHandler:
    encryption_key: Optional[str] = None
    if node.has_parameter("encryption_key"):
        encryption_key = node.get_parameter("encryption_key").value

    return MessageHandler(encryption_key)


def run_spin_in_thread(node):
    # Necessary in ROS2, else we get stuck
    thread = Thread(target=rclpy.spin, args=[node], daemon=True)
    thread.start()


def setup_udp_broadcast_socket() -> socket.socket:
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    return sock


def main():
    rclpy.init()
    node = Node("udp_bridge_sender", automatically_declare_parameters_from_overrides=True)
    run_spin_in_thread(node)

    if validate_params(node):
        port: int = node.get_parameter("port").value
        freq: float = node.get_parameter("send_frequency").value
        targets: list[str] = node.get_parameter("target_ips").value
        max_queue_size: int = node.get_parameter("sender_queue_max_size").value
        topics: list[str] = node.get_parameter("topics").value

        message_handler = setup_message_handler(node)
        sock = setup_udp_broadcast_socket()

        subscribers: list[AutoSubscriber] = list(
            map(lambda topic: AutoSubscriber(topic, max_queue_size, message_handler, node), topics)
        )

        while rclpy.ok():
            for subscriber in subscribers:
                try:
                    data = subscriber.queue.get_nowait()

                    for target in targets:
                        try:
                            sock.sendto(data + MessageHandler.PACKAGE_DELIMITER, (target, port))
                        except Exception as e:
                            node.get_logger().error(
                                f"Could not send data of topic {subscriber.topic} to {target} with error {str(e)}"
                            )

                except Empty:
                    pass

            node.get_clock().sleep_for(Duration(nanoseconds=int(1000000000 / freq)))
