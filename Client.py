import socket
import pickle
import numpy as np
from threading import Thread
import time

# Aqui a gente cria a lista de máquinas servidoras.
SERVERS = [('localhost', 5000), ('localhost', 5001)]


def send_submatrix(submatrix_a, matrix_b, primary_server_address, fallback_server_address_or_none, results, result_idx):
    current_target_address = primary_server_address
    attempt_description = "PRIMÁRIO"

    # Tenta conectar com os servidores
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(current_target_address)
            
            s.sendall(pickle.dumps((submatrix_a, matrix_b)))
            s.sendall(pickle.dumps((submatrix_a, matrix_b)))
            s.shutdown(socket.SHUT_WR) 

            data = b""
            while True:
                packet = s.recv(4096)
                if not packet:
                    break
                data += packet
            
            if not data:
                raise Exception("Nenhum dado recebido do servidor.")

            results[result_idx] = pickle.loads(data)
            return
            
    except (ConnectionRefusedError, socket.error, pickle.UnpicklingError, Exception) as e_primary:
        
        if fallback_server_address_or_none:
            current_target_address = fallback_server_address_or_none
            attempt_description = "FALLBACK"
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_fallback:
                    s_fallback.connect(current_target_address)
                    
                    s_fallback.sendall(pickle.dumps((submatrix_a, matrix_b)))
                    s_fallback.sendall(pickle.dumps((submatrix_a, matrix_b))) # Duplicate send
                    s_fallback.shutdown(socket.SHUT_WR)

                    data_fallback = b""
                    while True:
                        packet = s_fallback.recv(4096)
                        if not packet:
                            break
                        data_fallback += packet

                    if not data_fallback:
                        raise Exception("Nenhum dado recebido do servidor de fallback.")
                        
                    results[result_idx] = pickle.loads(data_fallback)
                    return 
            except (ConnectionRefusedError, socket.error, pickle.UnpicklingError, Exception) as e_fallback:
                print(f"Thread-{result_idx}: Falha com {attempt_description} {current_target_address} para a parte {result_idx}: {type(e_fallback).__name__} - {e_fallback}")
                results[result_idx] = None 
                print(f"Thread-{result_idx}: Todas as tentativas de conexão falharam para a parte {result_idx}. results[{result_idx}] = None")
                return
        else:
            results[result_idx] = None
            print(f"Thread-{result_idx}: Falha no primário e sem fallback disponível/aplicável para a parte {result_idx}. results[{result_idx}] = None")
            return

def main():
    start_time = time.time()
    
    # Gera as matriz
    A = np.random.randint(1, 10, (6, 4))  
    B = np.random.randint(1, 10, (4, 5))  

    print("Matriz A:\n", A)
    print("Matriz B:\n", B)

    num_splits = len(SERVERS)
    if num_splits == 0:
        print("Nenhum servidor configurado. Encerrando.")
        return
        
    parts = np.array_split(A, num_splits, axis=0)
    results = [None] * num_splits 
    threads = []

    print(f"\nDividindo Matriz A em {num_splits} parte(s).")

    for i in range(num_splits): 
        current_part_A = parts[i]
        primary_target_server = SERVERS[i] 
        
        fallback_target_server = None
        if num_splits > 1 and i == 1: 
            fallback_target_server = SERVERS[0]
        
        print(f"Configurando Thread-{i} para Parte-{i}: Primário={primary_target_server}, Fallback={fallback_target_server if fallback_target_server else 'Nenhum'}")
        t = Thread(target=send_submatrix, args=(current_part_A, B, primary_target_server, fallback_target_server, results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if any(r is None for r in results):
        print("\nErro: Uma ou mais partes da matriz não puderam ser processadas.")
        print("O resultado final está incompleto ou não pôde ser montado.")
        for i, r_val in enumerate(results):
            if r_val is None:
                print(f"  - Parte {i} falhou em ser processada.")
    else:
        try:
            final_result = np.vstack(results)
            print("\nResultado Final (A x B):\n", final_result)
        except ValueError as e:
            print(f"\nErro ao juntar os resultados: {e}")
            print("Isso pode acontecer se os formatos das submatrizes resultantes não forem compatíveis.")
            print("Resultados recebidos:", results)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nTempo de execução: {execution_time:.4f} segundos")

if __name__ == '__main__':
    main()
