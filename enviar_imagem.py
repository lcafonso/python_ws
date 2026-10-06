"""
Envia uma ou mais imagens para a fila 'imagens.pedidos'.
Os resultados (ficheiros JSON) da IA chegam à fila 'imagens.resultados'
e são gravados pelo script receber_json.py.

Uso:
    python enviar_imagem.py foto.jpg --plant plant_001
    python enviar_imagem.py a.jpg b.jpg --plant plant_002

O image_id é o nome do ficheiro sem extensão (ex.: img_001.jpg -> img_001).

Requer a variável de ambiente CLOUDAMQP_URL (URL amqps://... da consola CloudAMQP).
"""
import argparse
import hashlib
import mimetypes
import os
import sys
import uuid

import pika

URL = os.environ.get("CLOUDAMQP_URL")
FILA_PEDIDOS = "imagens.pedidos"
FILA_RESULTADOS = "imagens.resultados"


def main():
    ap = argparse.ArgumentParser(description="Envia imagens para análise")
    ap.add_argument("imagens", nargs="+", help="ficheiros de imagem")
    ap.add_argument("--plant", default="plant_001", help="identificador da planta (plant_id)")
    args = ap.parse_args()
    if not URL:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro (ver README.md).")

    ligacao = pika.BlockingConnection(pika.URLParameters(URL))
    canal = ligacao.channel()
    canal.queue_declare(queue=FILA_PEDIDOS, durable=True)
    # Fila de resultados durável: os JSON ficam guardados no broker
    # mesmo que o teu PC esteja desligado quando a IA responde.
    canal.queue_declare(queue=FILA_RESULTADOS, durable=True)
    canal.confirm_delivery()  # o broker confirma cada envio

    for caminho in args.imagens:
        with open(caminho, "rb") as f:
            dados = f.read()
        nome = os.path.basename(caminho)
        image_id = os.path.splitext(nome)[0]
        corr_id = str(uuid.uuid4())
        canal.basic_publish(
            exchange="",
            routing_key=FILA_PEDIDOS,
            body=dados,
            properties=pika.BasicProperties(
                content_type=mimetypes.guess_type(caminho)[0] or "application/octet-stream",
                correlation_id=corr_id,
                reply_to=FILA_RESULTADOS,
                delivery_mode=2,  # persistente
                headers={
                    "filename": nome,
                    "plant_id": args.plant,
                    "image_id": image_id,
                    "sha256": hashlib.sha256(dados).hexdigest(),
                },
            ),
        )
        print(f"Enviada: {nome}  plant_id={args.plant}  image_id={image_id}  "
              f"({len(dados) / 1024:.1f} KB)  id={corr_id[:8]}")

    ligacao.close()
    print("Os resultados vão aparecer na pasta do receber_json.py.")


if __name__ == "__main__":
    main()
