"""
TESTE DE COMUNICAÇÃO (modo por lotes) — LADO RCO, passo 1: enviar imagens.

Envia as imagens e termina. As imagens ficam no broker até o parceiro ligar o
programa dele (pode ser horas ou dias depois). Cada pedido enviado fica registado
na pasta 'pendentes/' para validar a resposta quando ela chegar.

Uso:
    python rco_enviar.py img1.jpg img2.jpg
    python rco_enviar.py pasta_com_imagens/
    python rco_enviar.py img1.jpg --object-id PLANT-000123

Variável de ambiente: CLOUDAMQP_URL=amqps://...
"""
import argparse
import glob
import io
import mimetypes
import os
import sys

from rco_comm import AIRequest, ImagePayload, TransportError
from rco_comm.batch import BatchSender

EXT = (".jpg", ".jpeg", ".png")


def dimensoes(dados):
    try:
        from PIL import Image
        with Image.open(io.BytesIO(dados)) as im:
            return im.size
    except Exception:
        return None, None


def main():
    ap = argparse.ArgumentParser(description="Envia imagens para análise (modo por lotes)")
    ap.add_argument("imagens", nargs="+", help="ficheiros ou pastas de imagens")
    ap.add_argument("--object-id", help="por defeito PLANT-<nome do ficheiro>")
    ap.add_argument("--camera-id", default="intervention_camera_01")
    ap.add_argument("--cell", default="cell_01")
    args = ap.parse_args()

    ficheiros = []
    for a in args.imagens:
        if os.path.isdir(a):
            ficheiros += sorted(p for p in glob.glob(os.path.join(a, "*")) if p.lower().endswith(EXT))
        else:
            ficheiros.append(a)
    if not ficheiros:
        sys.exit("Nenhuma imagem encontrada.")

    url = os.environ.get("CLOUDAMQP_URL")
    if not url:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro.")
    try:
        sender = BatchSender(url, args.cell)
    except TransportError as e:
        sys.exit(f"Erro: {e}")

    enviados = 0
    for caminho in ficheiros:
        with open(caminho, "rb") as f:
            dados = f.read()
        w, h = dimensoes(dados)
        if w is None:
            print(f"  ignorado (não é imagem): {caminho}")
            continue
        stem = os.path.splitext(os.path.basename(caminho))[0]
        req = AIRequest(
            object_id=args.object_id or f"PLANT-{stem}",
            image=ImagePayload(stem, args.camera_id,
                               (mimetypes.guess_type(caminho)[0] or "image/jpeg").split("/")[-1],
                               dados, w, h))
        try:
            sender.send(req, caminho)
        except TransportError as e:
            print(f"  ERRO a enviar {caminho}: {e}")
            continue
        enviados += 1
        print(f"Enviada: {caminho}  ({w}x{h}, {len(dados) / 1024:.0f} KB)  -> {req.request_id}")
    sender.close()
    print(f"\n{enviados} imagem(ns) enviada(s). Ficam no broker até o parceiro as processar.")
    print("Para recolher as respostas: python rco_receber.py")


if __name__ == "__main__":
    main()
