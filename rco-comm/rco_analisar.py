"""
LADO RCO (a tua máquina): envia a imagem da zona de intervenção para a IA,
espera pela resposta, valida-a e grava o JSON.

Uso:
    python rco_analisar.py imagem.jpg --object-id PLANT-000123
    python rco_analisar.py imagem.jpg --object-id PLANT-000123 --timeout 20 --tentativas 3
    python rco_analisar.py imagem.jpg --mock          # sem broker, IA simulada local

Variável de ambiente: CLOUDAMQP_URL=amqps://...

Código de saída: 0 = COMPLETED (resposta válida), 2 = INVALID, 3 = FAILED (timeout/erros).
"""
import argparse
import json
import mimetypes
import os
import sys

from rco_comm import AIRequest, ImagePayload, MockAIClient, ValidationRules


def dimensoes(dados):
    try:
        import io
        from PIL import Image
        with Image.open(io.BytesIO(dados)) as im:
            return im.size
    except Exception:
        return None, None


def main():
    ap = argparse.ArgumentParser(description="RCO: pedido de análise à IA externa")
    ap.add_argument("imagem")
    ap.add_argument("--object-id", default="PLANT-000001")
    ap.add_argument("--image-id", help="por defeito: nome do ficheiro sem extensão")
    ap.add_argument("--camera-id", default="intervention_camera_01")
    ap.add_argument("--cell", default="cell_01", help="identificador da célula (fila de respostas)")
    ap.add_argument("--timeout", type=float, default=30.0, help="segundos por tentativa")
    ap.add_argument("--tentativas", type=int, default=3)
    ap.add_argument("--min-confianca", type=float, default=0.70)
    ap.add_argument("--saida", default="resultados")
    ap.add_argument("--mock", action="store_true", help="usar IA simulada local (sem rede)")
    args = ap.parse_args()

    with open(args.imagem, "rb") as f:
        dados = f.read()
    w, h = dimensoes(dados)
    tipo = (mimetypes.guess_type(args.imagem)[0] or "image/jpeg").split("/")[-1]
    req = AIRequest(
        object_id=args.object_id,
        image=ImagePayload(
            image_id=args.image_id or os.path.splitext(os.path.basename(args.imagem))[0],
            camera_id=args.camera_id, encoding=tipo, data=dados, width=w, height=h),
    )

    def on_event(ev):
        extra = ", ".join(f"{k}={v}" for k, v in ev.data.items() if k != "errors")
        print(f"[{ev.timestamp[11:23]}] {ev.name:<28} {extra}")

    opts = dict(timeout_s=args.timeout, max_attempts=args.tentativas,
                rules=ValidationRules(min_confidence=args.min_confianca), on_event=on_event)
    if args.mock:
        client = MockAIClient(**opts)
    else:
        url = os.environ.get("CLOUDAMQP_URL")
        if not url:
            sys.exit("Define a variável CLOUDAMQP_URL (ou usa --mock).")
        from rco_comm.rabbitmq import RabbitMQAIClient
        client = RabbitMQAIClient(url, cell_id=args.cell, **opts)

    print(f"Pedido {req.request_id}: object={req.object_id} image={req.image.image_id} "
          f"({len(dados) / 1024:.0f} KB, {w}x{h})")
    with client:
        res = client.analyze(req)

    print(f"\nEstado final: {res.state.value}  (tentativas: {res.attempts})")
    print("Percurso: " + " -> ".join(s for s, _ in res.history))
    for e in res.errors:
        print(f"  ! {e}")

    if res.response is not None:
        os.makedirs(args.saida, exist_ok=True)
        sufixo = "" if res.ok else f"_{res.state.value}"
        destino = os.path.join(args.saida, f"{req.image.image_id}_{res.request_id}{sufixo}.json")
        with open(destino, "wb") as f:
            f.write(res.response.to_json(indent=2))
        print(f"JSON gravado: {destino}")
        if res.ok:
            for p in res.response.points:
                print(f"  ponto {p.id}: {p.type}  u={p.pixel.u:.0f} v={p.pixel.v:.0f}  conf={p.confidence:.2f}"
                      if p.pixel else f"  ponto {p.id}: {p.type} {p.position}")

    sys.exit({"COMPLETED": 0, "INVALID": 2}.get(res.state.value, 3))


if __name__ == "__main__":
    main()
