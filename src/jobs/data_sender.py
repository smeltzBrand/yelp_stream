import socket
import time

def send_data():
    server_address = ('localhost', 9999)
    data = [
        '{"review_id": "123", "user_id":"u1", "business_id": "b1", "stars": 4.5, "date": "2023-07-18", "text", "Great service!"}',
        '{"review_id": "124", "user_id":"u2", "business_id": "b2", "stars": 3.5, "date": "2023-07-18", "text", "Average Experience."}'
    ]

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(server_address)
        for line in data:
            s.sendall(line.encode('utf-8'))
            time.sleep(1) # Sleep to simulate stream

if __name__ == "__main__":
    send_data()