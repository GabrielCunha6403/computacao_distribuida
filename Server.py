import socket
import pickle
import numpy as np

from multiprocessing import Pool

HOST = 'localhost'

def get_available_port(base_port=5000) -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('localhost', base_port))
        s.close()
        return base_port
    except OSError:
        return base_port + 1
    
PORT = get_available_port()  # type: ignore # Altere para 5001 no segundo servidor

def multiply_row(args):
    row, matrix_b = args
    return np.dot(row, matrix_b)

def handle_client(conn):
    data = b""
    while True:
        packet = conn.recv(4096)
        if not packet:
            break
        data += packet
    submatrix_a, matrix_b = pickle.loads(data)

    # Multiplicação paralela
    with Pool() as pool:
        result = pool.map(multiply_row, [(row, matrix_b) for row in submatrix_a])

    result = np.array(result)
    print("Resultado parcial:\n", result)
    conn.sendall(pickle.dumps(result))
    conn.close()

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"Servidor ouvindo em {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            print(f"Conexão recebida de {addr}")
            handle_client(conn)

if __name__ == '__main__':
    start_server()
