import socket
import sys

def check_port(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((host, port))
        print(f"Port {port} is OPEN on {host}")
        return True
    except:
        print(f"Port {port} is CLOSED on {host}")
        return False
    finally:
        s.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 check_port.py <host> <port>")
    else:
        check_port(sys.argv[1], int(sys.argv[2]))
