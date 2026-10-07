"""
Desenha os pontos de uma resposta da IA sobre a imagem.

Uso:
    python visualizar.py imagem.jpg resultados/IMG-001_AI_REQUEST-XXXX.json [saida.png]
"""
import json
import os
import sys

from PIL import Image, ImageDraw


def main():
    if len(sys.argv) < 3:
        sys.exit("Uso: python visualizar.py imagem.jpg resposta.json [saida.png]")
    img_path, json_path = sys.argv[1], sys.argv[2]
    saida = sys.argv[3] if len(sys.argv) > 3 else f"{os.path.splitext(img_path)[0]}_anotada.png"
    with open(json_path, encoding="utf-8") as f:
        r = json.load(f)
    im = Image.open(img_path).convert("RGB")
    d = ImageDraw.Draw(im)
    raio = max(6, im.width // 80)
    for p in r.get("points", []):
        px = p.get("pixel")
        if not px:
            continue
        u, v = px["u"], px["v"]
        d.ellipse([u - raio, v - raio, u + raio, v + raio], outline=(230, 30, 30), width=max(2, raio // 3))
        d.text((u + raio + 3, v - raio), f'{p["id"]} {p["type"]} {p["confidence"]:.2f}', fill=(230, 30, 30))
    d.text((8, 8), f'{r.get("object_id")} / {r.get("image_id")} / {r.get("analysis_id")} '
                   f'[{r.get("status")}]', fill=(255, 255, 255))
    im.save(saida)
    print(f"Imagem anotada: {saida}")


if __name__ == "__main__":
    main()
