"""
IA SIMULADA de análise de plantas.

Fica à escuta na fila 'imagens.pedidos', recebe cada imagem, "analisa-a"
e publica um JSON com pontos semânticos da planta (em píxeis da imagem)
na fila indicada em 'reply_to' (por defeito 'imagens.resultados').

A análise é FALSA mas plausível:
  - segmenta a zona verde da imagem (a planta);
  - estima o caule (linha central da zona verde);
  - coloca 'cut_node' ao longo do caule e um 'grasp_point' na parte de baixo;
  - o resultado é determinístico: a mesma imagem dá sempre os mesmos pontos.

Uso:
    python ia_simulada.py                 # atraso simulado de inferência 0.5-2 s
    python ia_simulada.py --sem-atraso

Requer: pip install pika pillow
e a variável de ambiente CLOUDAMQP_URL (amqps://...).
"""
import argparse
import datetime as dt
import hashlib
import io
import json
import os
import random
import sys
import time

import pika
from PIL import Image, UnidentifiedImageError

URL = os.environ.get("CLOUDAMQP_URL")
FILA_PEDIDOS = "imagens.pedidos"
FILA_RESULTADOS = "imagens.resultados"
MODELO = "plant-sim-0.1"
LADO_ANALISE = 200  # a imagem é reduzida para este tamanho máximo para "analisar"


def texto(v):
    return v.decode("utf-8", "replace") if isinstance(v, bytes) else v


# ---------------------------------------------------------------------------
# Análise simulada
# ---------------------------------------------------------------------------
def mascara_verde(im):
    """Devolve (largura, altura, set de píxeis verdes) da imagem reduzida."""
    w, h = im.size
    px = im.load()
    verdes = set()
    for y in range(h):
        for x in range(w):
            r, g, b = px[x, y]
            if g > 50 and g > r + 15 and g > b + 15:
                verdes.add((x, y))
    return w, h, verdes


def estimar_caule(verdes, w, y_min, y_max):
    """
    Para cada linha y devolve o x do caule.
    O caule é vertical, por isso numa janela vertical à volta de y a coluna
    com mais píxeis verdes é o caule (as folhas espalham-se na horizontal).
    """
    altura = max(1, y_max - y_min)
    meia = max(2, int(altura * 0.12))
    # contagem de verdes por (linha, coluna), acumulada ao longo das linhas
    por_linha = [[0] * w for _ in range(y_max + 2)]
    for x, y in verdes:
        por_linha[y + 1][x] += 1
    for y in range(1, y_max + 2):
        ant, cur = por_linha[y - 1], por_linha[y]
        for x in range(w):
            cur[x] += ant[x]
    xs = {}
    anterior = None
    for y in range(y_max, y_min - 1, -1):  # de baixo (base) para cima
        a, b = max(0, y - meia), min(y_max, y + meia)
        contagem = [por_linha[b + 1][x] - por_linha[a][x] for x in range(w)]
        # suavizar em 3 colunas e preferir colunas perto do caule da linha anterior
        melhor, melhor_val = None, -1.0
        for x in range(w):
            val = sum(contagem[max(0, x - 1):x + 2])
            if anterior is not None:
                val -= 0.5 * abs(x - anterior)
            if val > melhor_val:
                melhor, melhor_val = x, val
        xs[y] = melhor
        anterior = melhor
    return xs


def analisar(dados: bytes, plant_id: str, image_id: str, seed: str) -> dict:
    rng = random.Random(seed)
    try:
        original = Image.open(io.BytesIO(dados))
    except UnidentifiedImageError:
        raise ValueError("o ficheiro recebido não é uma imagem válida")
    W, H = original.size
    im = original.convert("RGB")
    im.thumbnail((LADO_ANALISE, LADO_ANALISE))
    w, h, verdes = mascara_verde(im)
    escala = W / w

    planta_encontrada = len(verdes) >= 0.01 * w * h
    nos_y = []
    if planta_encontrada:
        ys = [y for _, y in verdes]
        y_min, y_max = min(ys), max(ys)
        altura = max(1, y_max - y_min)
        caule = estimar_caule(verdes, w, y_min, y_max)

        # "massa lateral": píxeis verdes perto do caule, fora do próprio caule.
        # Picos desta massa = folhas -> o nó fica onde a folha se liga ao caule.
        alcance = max(3, int(altura * 0.4))
        lateral = {y: 0 for y in range(y_min, y_max + 1)}
        for x, y in verdes:
            d = abs(x - caule[y])
            if 2 < d <= alcance:
                lateral[y] += 1
        base = sorted(lateral.values())[len(lateral) // 2]  # mediana
        dist_min = max(2, int(altura * 0.12))
        candidatos = sorted(lateral, key=lambda y: -lateral[y])
        for y in candidatos:
            if lateral[y] <= base + 2 or len(nos_y) >= 4:
                break
            if y > y_min + altura * 0.9:  # não pôr nós junto à base
                continue
            if all(abs(y - o) >= dist_min for o in nos_y):
                nos_y.append(y)
        nos_y.sort()
    else:
        # sem verde suficiente: assume uma planta vertical no centro da imagem
        y_min, y_max = int(h * 0.15), int(h * 0.9)
        altura = y_max - y_min
        caule = {y: w // 2 for y in range(y_min, y_max + 1)}

    if len(nos_y) < 2:  # poucos nós encontrados: distribuir ao longo do caule
        nos_y = [int(y_min + altura * f) for f in (0.2, 0.45, 0.7)]

    def px(x, y):
        return round(x * escala), round(y * escala)

    detections = []
    for i, y in enumerate(nos_y):
        x, y = px(caule[y], y)
        detections.append({
            "id": f"node_{i + 1:02d}",
            "type": "cut_node",
            "x": x,
            "y": y,
            "confidence": round(rng.uniform(0.82, 0.99), 2),
        })

    # ponto de preensão: zona de caule "limpa" (menos folhas) abaixo do último nó
    inicio = min(y_max, nos_y[-1] + max(2, int(altura * 0.1)))
    fim = max(inicio, y_max - max(1, int(altura * 0.05)))
    if planta_encontrada:
        gy = min(range(inicio, fim + 1), key=lambda y: (lateral[y], -y))
    else:
        gy = (inicio + fim) // 2
    gx, gy = px(caule[gy], gy)
    grasp_points = [{
        "id": "grasp_01",
        "x": gx,
        "y": gy,
        "confidence": round(rng.uniform(0.85, 0.98), 2),
    }]

    if not planta_encontrada:  # menor confiança quando não viu planta
        for p in detections + grasp_points:
            p["confidence"] = round(p["confidence"] * 0.5, 2)

    return {
        "plant_id": plant_id,
        "image_id": image_id,
        "status": "ok",
        "model": MODELO,
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "image_size": {"width": W, "height": H},
        "plant_found": planta_encontrada,
        "detections": detections,
        "grasp_points": grasp_points,
    }


# ---------------------------------------------------------------------------
# RabbitMQ
# ---------------------------------------------------------------------------
def criar_handler(atraso: bool):
    def on_pedido(ch, method, props, body):
        headers = props.headers or {}
        corr = props.correlation_id or "sem-id"
        nome = os.path.basename(texto(headers.get("filename")) or f"{corr}.bin")
        image_id = texto(headers.get("image_id")) or os.path.splitext(nome)[0]
        plant_id = texto(headers.get("plant_id")) or "plant_unknown"
        sha = hashlib.sha256(body).hexdigest()
        print(f"Recebida: {nome}  plant_id={plant_id}  image_id={image_id}  ({len(body) / 1024:.1f} KB)")

        try:
            if atraso:
                time.sleep(random.uniform(0.5, 2.0))  # simula o tempo de inferência
            resultado = analisar(body, plant_id, image_id, seed=sha)
            print(f"  -> {len(resultado['detections'])} cut_node, "
                  f"{len(resultado['grasp_points'])} grasp_point")
        except Exception as e:  # imagem inválida, etc.
            resultado = {"plant_id": plant_id, "image_id": image_id,
                         "status": "error", "model": MODELO, "error": str(e),
                         "detections": [], "grasp_points": []}
            print(f"  -> ERRO: {e}")

        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to or FILA_RESULTADOS,
            body=json.dumps(resultado, ensure_ascii=False, indent=4).encode("utf-8"),
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id,
                content_type="application/json",
                delivery_mode=2,
                headers={"filename": f"{image_id}_{corr[:8]}.json",
                         "plant_id": plant_id, "image_id": image_id},
            ),
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    return on_pedido


def main():
    ap = argparse.ArgumentParser(description="IA simulada de análise de plantas")
    ap.add_argument("--sem-atraso", action="store_true", help="não simular tempo de inferência")
    args = ap.parse_args()
    if not URL:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro (ver README.md).")

    params = pika.URLParameters(URL)
    params.heartbeat = 60
    ligacao = pika.BlockingConnection(params)
    canal = ligacao.channel()
    canal.queue_declare(queue=FILA_PEDIDOS, durable=True)
    canal.queue_declare(queue=FILA_RESULTADOS, durable=True)
    canal.basic_qos(prefetch_count=1)
    canal.basic_consume(queue=FILA_PEDIDOS, on_message_callback=criar_handler(not args.sem_atraso))

    print(f"IA simulada ({MODELO}) à escuta em '{FILA_PEDIDOS}'. Ctrl+C para sair.")
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        canal.stop_consuming()
    finally:
        ligacao.close()


if __name__ == "__main__":
    main()
