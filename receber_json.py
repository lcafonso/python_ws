"""
Recebe os ficheiros JSON enviados pela IA (fila 'imagens.resultados')
e grava cada um na pasta 'resultados'.

Uso:
    python receber_json.py

Deixar a correr. Se estiver parado, os JSON ficam guardados no broker
e são todos gravados quando o script voltar a arrancar.

Requer a variável de ambiente CLOUDAMQP_URL (URL amqps://... da consola CloudAMQP).
"""
import json
import os
import sys

import pika

URL = os.environ.get("CLOUDAMQP_URL")
FILA_RESULTADOS = "imagens.resultados"
PASTA = "resultados"


def nome_seguro(props):
    headers = props.headers or {}
    nome = headers.get("filename") or f"{(props.correlation_id or 'resultado')[:8]}.json"
    if isinstance(nome, bytes):
        nome = nome.decode("utf-8", "replace")
    nome = os.path.basename(nome)
    if not nome.lower().endswith(".json"):
        nome += ".json"
    return nome


def on_resultado(ch, method, props, body):
    nome = nome_seguro(props)
    destino = os.path.join(PASTA, nome)
    base, ext = os.path.splitext(destino)
    n = 1
    while os.path.exists(destino):  # não sobrescrever ficheiros anteriores
        destino = f"{base}_{n}{ext}"
        n += 1

    try:
        conteudo = json.loads(body)  # valida que é JSON
    except ValueError:
        print(f"AVISO: '{nome}' não é JSON válido; gravado tal como chegou.")
        conteudo = None

    with open(destino, "wb") as f:
        f.write(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)  # só apaga do broker depois de gravado

    print(f"\nRecebido: {destino}")
    if isinstance(conteudo, dict):
        print(f"  plant_id={conteudo.get('plant_id')}  image_id={conteudo.get('image_id')}  "
              f"status={conteudo.get('status')}")
        if conteudo.get("status") == "error":
            print(f"  erro: {conteudo.get('error')}")
        for p in conteudo.get("detections", []):
            print(f"  {p['id']:<9} {p['type']:<9} x={p['x']:>5} y={p['y']:>5}  conf={p['confidence']:.2f}")
        for p in conteudo.get("grasp_points", []):
            print(f"  {p['id']:<9} {'grasp':<9} x={p['x']:>5} y={p['y']:>5}  conf={p['confidence']:.2f}")


def main():
    if not URL:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro (ver README.md).")
    os.makedirs(PASTA, exist_ok=True)

    params = pika.URLParameters(URL)
    params.heartbeat = 60
    ligacao = pika.BlockingConnection(params)
    canal = ligacao.channel()
    canal.queue_declare(queue=FILA_RESULTADOS, durable=True)
    canal.basic_qos(prefetch_count=10)
    canal.basic_consume(queue=FILA_RESULTADOS, on_message_callback=on_resultado)

    print(f"À espera de ficheiros JSON na fila '{FILA_RESULTADOS}' -> pasta '{PASTA}'. Ctrl+C para sair.")
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        canal.stop_consuming()
    finally:
        ligacao.close()


if __name__ == "__main__":
    main()
