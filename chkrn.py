import socket
import sys
import time

def get_lock(process_name):
    global lock_socket   # Without this our lock gets garbage collected
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print('Checked if script was already running')
    except socket.error:
        print('Process {0} is already running, only one instance is allowed'.format(process_name))
        sys.exit()


get_lock('chk_if_running.py')
while True:
    time.sleep(3)
