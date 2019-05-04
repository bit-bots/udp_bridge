#!/usr/bin/env python3
import rospy
import rostopic
import socket
import pickle
import base64
from threading import Lock
from queue import Queue, Empty, Full

from udp_bridge.aes_helper import AESCipher


class UdpSender:
    def __init__(self, target_ip, port):
        rospy.init_node("udp_bridge_sender", anonymous=True, log_level=rospy.ERROR)
        rospy.loginfo("Initializing udp_bridge to '" + target_ip + ":" + str(port) + "'")

        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.target = (target_ip, port)

        self.cipher = AESCipher(rospy.get_param("udp_bridge/encryption_key", None))

        self.queue_lock = Lock()
        self.queues = {}
        self.max_queue_size = rospy.get_param('udp_bridge/sender_queue_max_size')
        self.subscribers = {}

        for topic in rospy.get_param("udp_bridge/topics"):
            self.setup_topic_subscriber(topic)

    def setup_topic_subscriber(self, topic, backoff=1.0):
        """Try to setup a subscriber for the specified topic until it works"""
        data_class, _, _ = rostopic.get_topic_class(topic)
        if data_class is not None:
            self.subscribers[topic] \
                = rospy.Subscriber(topic, data_class, self.topic_callback, topic, queue_size=2, tcp_nodelay=True)

            self.queue_lock.acquire()
            self.queues[topic] = Queue(self.max_queue_size)
            self.queue_lock.release()

            rospy.loginfo("Subscribed to topic " + topic)
        else:
            rospy.loginfo("Topic " + topic + " is not yet known. Retrying in " + str(int(backoff)) + " seconds")
            rospy.Timer(
                rospy.Duration(int(backoff)),
                lambda event: self.setup_topic_subscriber(topic, backoff * 1.2),
                oneshot=True)

    def topic_callback(self, data, topic):
        serialized_data = base64.b64encode(pickle.dumps({
            "data": data,
            "topic": topic
        }, pickle.HIGHEST_PROTOCOL)).decode("ASCII")
        enc_data = self.cipher.encrypt(serialized_data)
        try:
            self.queues[topic].put(enc_data, block=True, timeout=0.5)
        except Full:
            rospy.loginfo('Could not enqueue data for topic {}. Queue full'.format(topic))

    def process_queues_once(self):
        self.queue_lock.acquire()

        for topic, queue in self.queues.items():
            try:
                data = queue.get_nowait()
                self.sock.sendto(data +  b'\xff\xff\xff', self.target)
                queue.task_done()
            except Empty:
                pass
            except Exception as e:
                rospy.logwarn('Could not send data from topic {} to {} with error {}'.format(topic, self.target, str(e)))
                queue.task_done()

        self.queue_lock.release()


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

    if not rospy.has_param('udp_bridge/sender_queue_max_size'):
        rospy.logfatal('parameter \'sender_queue_max_size\' not found')
        result = False
    if not isinstance(rospy.get_param('udp_bridge/sender_queue_max_size'), int):
        rospy.logfatal('parameter \'sender_queue_max_size\' is not an Integer')
        result = False

    if not rospy.has_param('udp_bridge/send_frequency'):
        rospy.logfatal('parameter \'send_frequency\' not found')
        result = False
    if not isinstance(rospy.get_param('udp_bridge/send_frequency'), float) \
        and not isinstance(rospy.get_param('udp_bridge/send_frequency'), int):
        rospy.logfatal('parameter \'send_frequency\' is not an Integer or Float')
        result = False

    return result


if __name__ == '__main__':
    if validate_params():
        port = rospy.get_param("udp_bridge/port")
        freq = rospy.get_param("udp_bridge/send_frequency")
        senders = []
        for ip in rospy.get_param("udp_bridge/target_ips"):
            senders.append(UdpSender(ip, port))

        while not rospy.is_shutdown():
            for sender in senders:
                sender.process_queues_once()

            rospy.sleep(rospy.Duration(0, int(1000000000 / freq)))

