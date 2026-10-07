# Serviço de análise de imagens — lado do parceiro (IA)

Este pacote recebe imagens de plantas através de um broker RabbitMQ (CloudAMQP), analisa-as e devolve, para cada imagem, um ficheiro JSON com os pontos de corte.

Não é preciso abrir portas nem configurar o router: o programa só faz ligações de saída para o broker.

## 1. Instalação

Python 3.9 ou superior:
```
pip install -r requirements.txt
```

## 2. Configurar o acesso ao broker

O URL de acesso (`amqps://...`) é enviado separadamente.

Linux/macOS:
```
export CLOUDAMQP_URL='amqps://...'
```
Windows PowerShell:
```
$env:CLOUDAMQP_URL = "amqps://..."
```

## 3. Primeiro teste de comunicação (com a IA simulada)

As imagens já foram (ou vão ser) enviadas por nós e estão à espera no broker. Para as processar:

```
python parceiro_ia.py --lote --sair-quando-vazio --guardar recebidas
```

O programa:
- mostra quantas imagens estão pendentes;
- analisa cada uma com uma **IA simulada** (já incluída) e envia o JSON de resposta;
- guarda as imagens recebidas e as respostas na pasta `recebidas/`;
- termina quando já não houver imagens pendentes.

Pode ser corrido a qualquer momento: as imagens ficam guardadas no broker até serem processadas.

Este primeiro teste serve só para validar a comunicação entre as duas máquinas. Não é preciso alterar nada no código.

## 4. Ligar o modelo real

Substituir a função `analisar(imagem, pedido)` em `parceiro_ia.py`.

- `imagem`: bytes da imagem (JPEG/PNG)
- `pedido`: dados do pedido (`pedido.object_id`, `pedido.image.image_id`, `pedido.image.width`, `pedido.image.height`, ...)

A função devolve um dicionário:

```python
{
    "intervention_type": "cut",
    "confidence": 0.94,                 # confiança global, 0 a 1
    "points": [
        {"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96},
        {"id": 2, "type": "cut_node", "pixel": {"u": 510, "v": 276}, "confidence": 0.91}
    ],
    "model": {"name": "nome-do-modelo", "version": "1.0"}
}
```

- `u`, `v`: coordenadas em píxeis na imagem recebida, origem no canto superior esquerdo, `u` para a direita, `v` para baixo.
- Opcional, se o modelo tiver profundidade: `"position": {"x": ..., "y": ..., "z": ...}` (metros, no referencial da câmara).
- Se a imagem for inutilizável: `raise InvalidImageError("motivo")`. Qualquer outra exceção é tratada como falha temporária.

Tudo o resto (receção, verificação da integridade da imagem, envio da resposta, tratamento de erros, religação ao broker) é feito pelo pacote. A saída da função é verificada contra o contrato antes de ser enviada.

Exemplos de pedido e resposta completos em `exemplos/`.

## 5. Modo contínuo (fase seguinte)

Mais tarde, com a célula robótica a funcionar em tempo real, o programa fica sempre ligado:
```
python parceiro_ia.py
```
Neste modo cada análise tem de responder dentro do tempo limite definido pela célula (por defeito 30 s).

## Opções úteis

- `--guardar PASTA` — guarda as imagens recebidas e as respostas
- `--atraso N` — simula N segundos de inferência
- `--falha erro|invalido|silencio|aleatoria` — simula falhas (para testarmos o nosso lado)
