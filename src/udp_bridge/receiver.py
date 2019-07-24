#!/usr/bin/env python3
import termios
import sys
import tty
import rospy
import socket
import pickle
import base64
import select
import time
from threading import Thread
from udp_bridge.aes_helper import AESCipher


class UdpReceiver:
    def __init__(self):
        port = rospy.get_param("udp_bridge/port")
        rospy.init_node("udp_bridge_receiver", log_level=rospy.ERROR)
        rospy.loginfo("Initializing udp_bridge on port " + str(port))

        self.sock = socket.socket(type=socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))

        self.remap = None  # type: str
        self.known_senders = []  # type: list

        self.cipher = AESCipher(rospy.get_param("udp_bridge/encryption_key", None))

        self.publishers = {}

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

            if hostname not in self.known_senders:
                self.known_senders.append(hostname)

            self.publish(topic, data, hostname)
        except Exception as e:
            rospy.logerr('Could not deserialize received message with error {}'.format(str(e)))

    def publish(self, topic, msg, hostname):
        # publish msg under namespace
        namespaced_topic = "{}/{}".format(hostname, topic)
        if namespaced_topic not in self.publishers.keys():
            rospy.loginfo('Publishing new topic {}:{}'.format(hostname, topic))
            self.publishers[namespaced_topic] = rospy.Publisher(namespaced_topic, type(msg), tcp_nodelay=True,
                                                                queue_size=5)

        self.publishers[namespaced_topic].publish(msg)

        # publish topic under / as well if configured
        if self.remap is not None and self.remap == hostname:
            if topic not in self.publishers.keys():
                rospy.loginfo("Publishing new topic {}".format(topic))
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


def get_key():
    settings = termios.tcgetattr(sys.stdin)
    tty.setraw(sys.stdin.fileno())
    select.select([sys.stdin], [], [], 0)
    key = sys.stdin.read(1)
    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, settings)

    return key


def print_tui(receiver):
    while not rospy.is_shutdown():
        print("\033c", end="")
        print('\033[1m##### UDP Bridge #####\033[0m')
        print()
        print("Available namespaces to remap to /:")
        if len(receiver.known_senders) == 0:
            print("\033[91mNone\033[0m")
        else:
            for i, host in enumerate(receiver.known_senders):
                if host == receiver.remap:
                    print("\033[92m\033[1m    --> ({}) {} <--\033[0m".format(i, host))
                else:
                    print("        ({}) {}".format(i, host))
        print()
        print("To remap a namespace to / just press the corresponding number")

        rospy.sleep(rospy.Duration(1))


def main():
    if validate_params():
        # setup udp receiver
        receiver = UdpReceiver()
        udp_worker = Thread(target=receiver.recv_messages)
        udp_worker.setDaemon(False)

        tui_worker = Thread(target=print_tui, args=[receiver])
        tui_worker.setDaemon(True)

        udp_worker.start()
        tui_worker.start()

        while not rospy.is_shutdown():
            key = get_key()
            rospy.sleep(rospy.Duration(1))

            try:
                receiver.remap = receiver.known_senders[int(key)]
            # it can either throw ValueError or ListIndexOutOfRange
            except Exception:
                pass


if __name__ == '__main__':
    main()
