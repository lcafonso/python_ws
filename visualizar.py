"""
Desenha os pontos de um JSON de resultado por cima da imagem original.

Uso:
    python visualizar.py img_001.jpg resultados/img_001_1a2b3c4d.json
    -> cria img_001_anotada.png

Legenda: círculo vermelho = cut_node, quadrado azul = grasp_point.
Requer: pip install pillow
"""
import json
import os
import sys

from PIL import Image, ImageDraw


def main():
    if len(sys.argv) < 3:
        sys.exit("Uso: python visualizar.py imagem.jpg resultado.json [saida.png]")
    img_path, json_path = sys.argv[1], sys.argv[2]
    saida = sys.argv[3] if len(sys.argv) > 3 else f"{os.path.splitext(img_path)[0]}_anotada.png"

    with open(json_path, encoding="utf-8") as f:
        r = json.load(f)
    im = Image.open(img_path).convert("RGB")
    d = ImageDraw.Draw(im)
    raio = max(6, im.width // 80)
    esp = max(2, raio // 3)

    for p in r.get("detections", []):
        x, y = p["x"], p["y"]
        d.ellipse([x - raio, y - raio, x + raio, y + raio], outline=(230, 30, 30), width=esp)
        d.text((x + raio + 3, y - raio), f'{p["id"]} {p["confidence"]:.2f}', fill=(230, 30, 30))
    for p in r.get("grasp_points", []):
        x, y = p["x"], p["y"]
        d.rectangle([x - raio, y - raio, x + raio, y + raio], outline=(30, 90, 230), width=esp)
        d.text((x + raio + 3, y - raio), f'{p["id"]} {p["confidence"]:.2f}', fill=(30, 90, 230))

    d.text((8, 8), f'{r.get("plant_id")} / {r.get("image_id")}  [{r.get("model", "")}]', fill=(255, 255, 255))
    im.save(saida)
    print(f"Imagem anotada: {saida}")


if __name__ == "__main__":
    main()
