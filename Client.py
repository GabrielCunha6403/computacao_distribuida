import socket
import pickle
import numpy as np
from threading import Thread

SERVERS = [('localhost', 5000), ('localhost', 5001)]  # Dois servidores

def send_submatrix(submatrix_a, matrix_b, server_address, results, index):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(server_address)
        s.sendall(pickle.dumps((submatrix_a, matrix_b)))
        s.sendall(pickle.dumps((submatrix_a, matrix_b)))
        s.shutdown(socket.SHUT_WR)  # <-- esta linha força o servidor a sair do loop de recv


        data = b""
        while True:
            packet = s.recv(4096)
            if not packet:
                break
            data += packet
        results[index] = pickle.loads(data)
        print(f"Servidor {server_address} retornou:\n", results[index])

def main():
    # Gerar matrizes A e B
    A = np.random.randint(1, 10, (6, 4))  # Ex: 6x4
    B = np.random.randint(1, 10, (4, 5))  # Ex: 4x5 -> Resultado será 6x5

    print("Matriz A:\n", A)
    print("Matriz B:\n", B)

    # Dividir A em partes iguais para os servidores
    parts = np.array_split(A, len(SERVERS), axis=0)
    results = [None] * len(SERVERS)
    threads = []

    # Enviar para os servidores
    for i, (server_address, part) in enumerate(zip(SERVERS, parts)):
        t = Thread(target=send_submatrix, args=(part, B, server_address, results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Juntar os resultados
    final_result = np.vstack(results)
    print("Resultado Final (A x B):\n", final_result)

if __name__ == '__main__':
    main()
