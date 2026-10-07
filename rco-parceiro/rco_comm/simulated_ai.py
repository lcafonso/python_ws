"""
Analisador SIMULADO de plantas (substitui o modelo do parceiro durante o desenvolvimento).

Devolve pontos de corte ('cut_node') em píxeis da imagem:
  1. segmenta os píxeis verdes;
  2. estima o caule (colunas verticais dominantes);
  3. coloca um nó de corte no caule à altura de cada folha.
É determinístico: a mesma imagem dá sempre o mesmo resultado.

Formato devolvido (o mesmo que o analisador real do parceiro deve devolver):
    {
      "intervention_type": "cut",
      "confidence": 0.93,
      "points": [{"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96}],
      "model": {"name": "plant-sim", "version": "0.2"}
    }
"""
from __future__ import annotations

import io
import random

from PIL import Image, UnidentifiedImageError

MODEL = {"name": "plant-sim", "version": "0.2"}
LADO_ANALISE = 200


class InvalidImageError(ValueError):
    pass


def _mascara_verde(im):
    w, h = im.size
    px = im.load()
    verdes = []
    for y in range(h):
        for x in range(w):
            r, g, b = px[x, y]
            if g > 50 and g > r + 15 and g > b + 15:
                verdes.append((x, y))
    return w, h, verdes


def _estimar_caule(verdes, w, y_min, y_max):
    altura = max(1, y_max - y_min)
    meia = max(2, int(altura * 0.12))
    acum = [[0] * w for _ in range(y_max + 2)]
    for x, y in verdes:
        acum[y + 1][x] += 1
    for y in range(1, y_max + 2):
        ant, cur = acum[y - 1], acum[y]
        for x in range(w):
            cur[x] += ant[x]
    xs, anterior = {}, None
    for y in range(y_max, y_min - 1, -1):
        a, b = max(0, y - meia), min(y_max, y + meia)
        cont = [acum[b + 1][x] - acum[a][x] for x in range(w)]
        melhor, melhor_val = None, -1e9
        for x in range(w):
            val = sum(cont[max(0, x - 1):x + 2])
            if anterior is not None:
                val -= 0.5 * abs(x - anterior)
            if val > melhor_val:
                melhor, melhor_val = x, val
        xs[y] = melhor
        anterior = melhor
    return xs


def analisar_imagem(dados: bytes, seed: str = "") -> dict:
    rng = random.Random(seed or dados[:64])
    try:
        original = Image.open(io.BytesIO(dados))
        original.load()
    except (UnidentifiedImageError, OSError):
        raise InvalidImageError("o ficheiro recebido não é uma imagem válida")
    W, H = original.size
    im = original.convert("RGB")
    im.thumbnail((LADO_ANALISE, LADO_ANALISE))
    w, h, verdes = _mascara_verde(im)
    escala = W / w

    planta = len(verdes) >= 0.01 * w * h
    nos_y = []
    if planta:
        ys = [y for _, y in verdes]
        y_min, y_max = min(ys), max(ys)
        altura = max(1, y_max - y_min)
        caule = _estimar_caule(verdes, w, y_min, y_max)
        alcance = max(3, int(altura * 0.4))
        lateral = {y: 0 for y in range(y_min, y_max + 1)}
        for x, y in verdes:
            if 2 < abs(x - caule[y]) <= alcance:
                lateral[y] += 1
        base = sorted(lateral.values())[len(lateral) // 2]
        dist_min = max(2, int(altura * 0.12))
        for y in sorted(lateral, key=lambda k: -lateral[k]):
            if lateral[y] <= base + 2 or len(nos_y) >= 4:
                break
            if y > y_min + altura * 0.9:
                continue
            if all(abs(y - o) >= dist_min for o in nos_y):
                nos_y.append(y)
        nos_y.sort()
    else:
        y_min, y_max = int(h * 0.15), int(h * 0.9)
        altura = y_max - y_min
        caule = {y: w // 2 for y in range(y_min, y_max + 1)}

    if len(nos_y) < 2:
        nos_y = [int(y_min + altura * f) for f in (0.2, 0.45, 0.7)]

    pontos = []
    for i, y in enumerate(nos_y):
        conf = round(rng.uniform(0.82, 0.99), 2)
        if not planta:
            conf = round(conf * 0.5, 2)
        pontos.append({
            "id": i + 1,
            "type": "cut_node",
            "pixel": {"u": round(caule[y] * escala), "v": round(y * escala)},
            "confidence": conf,
        })

    return {
        "intervention_type": "cut",
        "confidence": round(min(p["confidence"] for p in pontos), 2),
        "points": pontos,
        "image_size": {"width": W, "height": H},
        "model": MODEL,
    }
