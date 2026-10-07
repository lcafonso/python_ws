"""
LADO PARCEIRO (máquina da IA): recebe as imagens, analisa-as e devolve o JSON.

Uso:
    python parceiro_ia.py                       # IA simulada
    python parceiro_ia.py --guardar recebidas   # guarda imagens e respostas numa pasta
    python parceiro_ia.py --atraso 5            # simula inferência lenta (5 s)
    python parceiro_ia.py --falha erro          # modos de falha para testar o RCO

    Teste de comunicação (modo por lotes, imagens enviadas com rco_enviar.py):
    python parceiro_ia.py --lote --sair-quando-vazio --guardar recebidas

Variável de ambiente: CLOUDAMQP_URL=amqps://...

>>> INTEGRAÇÃO DO MODELO REAL <<<
Substituir a função `analisar()` abaixo. Recebe os bytes da imagem e o pedido
(object_id, image_id, frame_id, ...) e devolve um dict:

    {
      "intervention_type": "cut",
      "confidence": 0.94,
      "points": [
        {"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96},
        {"id": 2, "type": "cut_node", "pixel": {"u": 510, "v": 276}, "confidence": 0.91}
      ],
      "model": {"name": "nome-do-modelo", "version": "1.0"}
    }

Opcional por ponto, se o modelo tiver profundidade:
    "position": {"x": 0.631, "y": 0.214, "z": 0.452}            (metros, no frame_id)
    "orientation": {"qx": 0, "qy": 0, "qz": 0.707, "qw": 0.707}

Erros: `raise InvalidImageError("...")` para imagem inutilizável (o RCO não repete);
qualquer outra exceção é tratada como falha temporária (o RCO pode repetir).
"""
import argparse
import os
import random
import sys
import time

from rco_comm.rabbitmq import AIServiceWorker
from rco_comm.simulated_ai import InvalidImageError, analisar_imagem  # noqa: F401

ARGS = None


def analisar(imagem: bytes, pedido) -> dict:
    """Substituir pelo modelo real. Por agora usa a IA simulada."""
    if ARGS.atraso:
        time.sleep(ARGS.atraso)
    falha = ARGS.falha
    if falha == "aleatoria":
        falha = random.choice(["nenhuma", "nenhuma", "erro", "invalido", "silencio"])
    if falha == "erro":
        raise RuntimeError("falha simulada do modelo")
    if falha == "silencio":
        time.sleep(120)  # responde tarde demais -> o RCO entra em TIMEOUT
    resultado = analisar_imagem(imagem, pedido.image.sha256)
    if falha == "invalido":  # resposta bem formada mas que o RCO deve rejeitar
        resultado["confidence"] = 0.2
        resultado["points"][0]["pixel"]["u"] = -50
    return resultado


def main():
    global ARGS
    ap = argparse.ArgumentParser(description="Serviço de IA do parceiro (RabbitMQ)")
    ap.add_argument("--guardar", metavar="PASTA", help="guardar imagens recebidas e respostas")
    ap.add_argument("--atraso", type=float, default=0.0, help="segundos de inferência simulada")
    ap.add_argument("--falha", default="nenhuma",
                    choices=["nenhuma", "erro", "invalido", "silencio", "aleatoria"])
    ap.add_argument("--lote", action="store_true",
                    help="modo de teste por lotes: processa as imagens pendentes (enviadas com rco_enviar.py)")
    ap.add_argument("--sair-quando-vazio", action="store_true",
                    help="terminar quando não houver mais imagens pendentes")
    ARGS = ap.parse_args()

    url = os.environ.get("CLOUDAMQP_URL")
    if not url:
        sys.exit("Define a variável CLOUDAMQP_URL primeiro.")
    worker = AIServiceWorker(url, analisar, save_dir=ARGS.guardar, batch=ARGS.lote,
                             idle_exit_s=5.0 if ARGS.sair_quando_vazio else None)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        worker.stop()
        print("\nParado.")


if __name__ == "__main__":
    main()
