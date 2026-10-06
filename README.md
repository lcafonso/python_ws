# Análise de imagens de plantas via RabbitMQ (CloudAMQP)

```
[PC]  enviar_imagem.py ──► imagens.pedidos ────► IA (ia_simulada.py / modelo real)
[PC]  receber_json.py  ◄── imagens.resultados ◄── ficheiro JSON com pontos da planta
```

A IA devolve **pontos semânticos da planta em coordenadas da imagem** (píxeis).
A conversão destes pontos em posições/poses do robô é feita do lado do robô (calibração da câmara + profundidade), não pela IA.

## Ficheiros

| Ficheiro | Lado | Função |
|---|---|---|
| `enviar_imagem.py` | cliente | envia imagens com `plant_id` e `image_id` |
| `receber_json.py` | cliente | recebe os JSON e grava-os em `resultados/` |
| `visualizar.py` | cliente | desenha os pontos do JSON sobre a imagem |
| `ia_simulada.py` | IA | IA falsa para testes; a IA real deve seguir o mesmo protocolo |
| `exemplo_*` | — | imagem de teste, JSON de resultado e imagem anotada |

## Instalação e configuração

```
pip install pika pillow
```

PowerShell:
```
$env:CLOUDAMQP_URL = "amqps://UTILIZADOR:PASSWORD@cow.rmq2.cloudamqp.com/VHOST"
```
Linux/macOS:
```
export CLOUDAMQP_URL="amqps://UTILIZADOR:PASSWORD@cow.rmq2.cloudamqp.com/VHOST"
```
Não colocar a password dentro do código nem em repositórios.

## Utilização

Terminal 1 — IA (simulada):
```
python ia_simulada.py              # com atraso de inferência simulado (0.5–2 s)
python ia_simulada.py --sem-atraso
```
Terminal 2 — receção dos resultados:
```
python receber_json.py
```
Terminal 3 — envio:
```
python enviar_imagem.py img_001.jpg --plant plant_001
python enviar_imagem.py a.jpg b.jpg c.jpg --plant plant_002
```
Ver o resultado sobre a imagem:
```
python visualizar.py img_001.jpg resultados/img_001_xxxxxxxx.json
```
(círculo vermelho = `cut_node`, quadrado azul = `grasp_point`)

## Protocolo

### Pedido — fila `imagens.pedidos` (durável)
- corpo: bytes da imagem (JPEG/PNG/...)
- `content_type`: tipo MIME da imagem
- `correlation_id`: UUID único por pedido
- `reply_to`: `imagens.resultados`
- headers: `filename`, `plant_id`, `image_id` (nome do ficheiro sem extensão), `sha256`

### Resposta — fila indicada em `reply_to` (durável)
- corpo: JSON UTF-8
- `content_type`: `application/json`
- `correlation_id`: o mesmo do pedido
- headers: `filename` (ex. `img_001_1a2b3c4d.json`), `plant_id`, `image_id`

```json
{
    "plant_id": "plant_001",
    "image_id": "img_001",
    "status": "ok",
    "model": "plant-sim-0.1",
    "timestamp": "2026-09-28T09:57:13+00:00",
    "image_size": { "width": 1024, "height": 768 },
    "plant_found": true,
    "detections": [
        { "id": "node_01", "type": "cut_node", "x": 456, "y": 184, "confidence": 0.96 }
    ],
    "grasp_points": [
        { "id": "grasp_01", "x": 548, "y": 686, "confidence": 0.86 }
    ]
}
```

Campos:
- `x`, `y`: píxeis na imagem **original**, origem no canto superior esquerdo, `x` para a direita, `y` para baixo.
- `confidence`: 0–1.
- `image_size`: dimensões da imagem analisada, para validar/escalar as coordenadas.
- `detections[].type`: por agora só `cut_node`; podem ser acrescentados outros tipos.
- Em caso de erro: `"status": "error"`, campo `"error"` com a mensagem, listas vazias.

## Como funciona a simulação

`ia_simulada.py` não usa nenhum modelo real:
1. segmenta os píxeis verdes (planta);
2. estima o caule pelas colunas verticais com mais verde;
3. coloca `cut_node` no caule à altura de cada folha (picos de verde lateral);
4. coloca o `grasp_point` na zona de caule com menos folhas, abaixo do último nó.

Sem planta visível, devolve pontos no centro com confiança baixa e `plant_found: false`. A mesma imagem dá sempre os mesmos pontos.

## Notas para o grupo de IA

- A imagem só é confirmada (`ack`) depois de o JSON ser enviado. Se o processo falhar, a imagem volta à fila.
- Se a inferência demorar vários minutos, correr o modelo numa thread separada para não interromper o heartbeat da ligação.
- Para processar em paralelo, basta lançar várias instâncias: o RabbitMQ distribui as imagens entre elas.
