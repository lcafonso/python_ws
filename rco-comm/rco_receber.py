"""
TESTE DE COMUNICAÇÃO (modo por lotes) — LADO RCO, passo 2: recolher as respostas.

Liga-se ao broker, recolhe todas as respostas que o parceiro já enviou, valida cada
uma contra o pedido original, grava o JSON da IA e converte os pontos em coordenadas
do braço. Pode ser corrido a qualquer momento, quantas vezes for preciso.

Uso:
    python rco_receber.py                                    # recolhe o que houver e termina
    python rco_receber.py --continuo                         # fica à escuta (Ctrl+C para sair)
    python rco_receber.py --calibracao calibracao_exemplo.json

Saída (pasta 'resultados/'):
    <image_id>_<request_id>.json         resposta da IA (píxeis)
    <image_id>_<request_id>_robot.json   pontos em coordenadas do braço (se houver calibração)
    <image_id>_<request_id>_anotada.png  imagem com os pontos (se a imagem original existir)

Variável de ambiente: CLOUDAMQP_URL=amqps://...
"""
import argparse
import json
import os
import sys

from rco_comm import TransportError, ValidationRules
from rco_comm.batch import BatchReceiver, PendingStore


def anotar(img_path, resp_dict, destino):
    try:
        from PIL import Image, ImageDraw
        im = Image.open(img_path).convert("RGB")
    except Exception:
        return False
    d = ImageDraw.Draw(im)
    r = max(6, im.width // 80)
    for p in resp_dict.get("points", []):
        px = p.get("pixel")
        if px:
            u, v = px["u"], px["v"]
            d.ellipse([u - r, v - r, u + r, v + r], outline=(230, 30, 30), width=max(2, r // 3))
            d.text((u + r + 3, v - r), f'{p["id"]} {p["confidence"]:.2f}', fill=(230, 30, 30))
    im.save(destino)
    return True


def main():
    ap = argparse.ArgumentParser(description="Recolhe as respostas da IA (modo por lotes)")
    ap.add_argument("--cell", default="cell_01")
    ap.add_argument("--continuo", action="store_true", help="ficar à escuta até Ctrl+C")
    ap.add_argument("--esperar", type=float, default=3.0,
                    help="segundos sem mensagens antes de terminar (por defeito 3)")
    ap.add_argument("--calibracao", default="calibracao_exemplo.json",
                    help="ficheiro de calibração câmara->braço ('' para não converter)")
    ap.add_argument("--min-confianca", type=float, default=0.70)
    ap.add_argument("--saida", default="resultados")
    args = ap.parse_args()

    url = os.environ.get("CLOUDAMQP_URL")
    if not url:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro.")

    cal = None
    if args.calibracao and os.path.exists(args.calibracao):
        from rco_comm.coordenadas import Calibration, response_to_robot
        cal = Calibration.load(args.calibracao)
        if "EXEMPLO" in cal.description.upper():
            print("Aviso: a usar a calibração de EXEMPLO (valores fictícios).\n")

    os.makedirs(args.saida, exist_ok=True)
    store = PendingStore()
    rx = BatchReceiver(url, args.cell, store, ValidationRules(min_confidence=args.min_confianca))
    contagem = {"VÁLIDA": 0, "INVÁLIDA": 0, "ERRO DA IA": 0, "DESCONHECIDA": 0}

    def tratar(r):
        resp = r.response
        base = f"{resp.image_id}_{r.request_id}" if resp else r.request_id
        if r.format_error:
            estado = "INVÁLIDA"
        elif not r.known:
            estado = "DESCONHECIDA"
        elif resp.status == "error":
            estado = "ERRO DA IA"
        else:
            estado = "VÁLIDA" if r.validation.valid else "INVÁLIDA"
        contagem[estado] += 1

        sufixo = "" if estado == "VÁLIDA" else "_" + {"INVÁLIDA": "INVALIDA", "ERRO DA IA": "ERRO",
                                                     "DESCONHECIDA": "DESCONHECIDA"}[estado]
        with open(os.path.join(args.saida, f"{base}{sufixo}.json"), "wb") as f:
            f.write(resp.to_json(indent=2) if resp else r.raw)

        print(f"[{estado}] {r.request_id}  object={resp.object_id if resp else '?'}  "
              f"image={resp.image_id if resp else '?'}")
        if r.format_error:
            print(f"    ! fora do contrato: {r.format_error}")
        if resp and resp.status == "error":
            print(f"    ! {resp.error.code}: {resp.error.message}")
        if r.validation and not r.validation.valid:
            for e in r.validation.errors:
                print(f"    ! {e}")

        if estado == "VÁLIDA":
            for p in resp.points:
                pix = f"u={p.pixel.u:.0f} v={p.pixel.v:.0f}" if p.pixel else ""
                print(f"    ponto {p.id} {p.type} {pix} conf={p.confidence:.2f}")
            if cal:
                try:
                    robot = response_to_robot(resp, cal)
                    with open(os.path.join(args.saida, f"{base}_robot.json"), "w", encoding="utf-8") as f:
                        json.dump(robot, f, indent=2, ensure_ascii=False)
                    for p in robot["points"]:
                        q = p["position"]
                        print(f"      -> braço ({robot['frame_id']}): x={q['x']:.3f} y={q['y']:.3f} z={q['z']:.3f} m")
                except ValueError as e:
                    print(f"    ! conversão de coordenadas: {e}")
            if r.local and anotar(r.local["image_path"], json.loads(resp.to_json()),
                                  os.path.join(args.saida, f"{base}_anotada.png")):
                print(f"    imagem anotada: {base}_anotada.png")

    try:
        print(f"Respostas à espera no broker: {rx.pending_on_broker()}\n")
        rx.collect(wait_s=None if args.continuo else args.esperar, on_result=tratar)
    except TransportError as e:
        sys.exit(f"Erro: {e}")
    except KeyboardInterrupt:
        print("\nParado.")

    print("\nResumo: " + ", ".join(f"{k.lower()}={v}" for k, v in contagem.items()))
    falta = store.list()
    print(f"Pedidos ainda sem resposta: {len(falta)}")
    for rid in falta:
        print(f"    {rid}")
    print(f"Resultados em: {os.path.abspath(args.saida)}")


if __name__ == "__main__":
    main()
