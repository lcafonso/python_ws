# Camada de comunicação RCO ↔ IA externa

Implementação da comunicação entre o **Robot Cell Orchestrator (RCO)** e a **IA do parceiro**, conforme as secções 11, 12, 15, 18 e 21 do relatório *"Arquitetura do Robot Cell Orchestrator"*.

```
 MÁQUINA RCO (nós)                 CloudAMQP (broker)                    MÁQUINA DO PARCEIRO
 ─────────────────                 ──────────────────                    ───────────────────
 rco_analisar.py                                                          parceiro_ia.py
   IAIClient ──AI_ANALYSIS_REQUEST──► rco.ai.requests ───────────────────► AIServiceWorker
   (máquina de estados,                                                     └─ analisar()  ← modelo
    validação, retries) ◄─AI_ANALYSIS_RESPONSE── rco.ai.responses.<cell> ◄──┘
```

As duas máquinas só fazem **ligações de saída** para o broker: nenhuma precisa de abrir portas no router.

Há dois modos, com o mesmo contrato de mensagens e a mesma validação:
- **Modo de teste por lotes** (assíncrono): para os testes iniciais de comunicação. Nenhum dos lados precisa de estar ligado ao mesmo tempo.
- **Modo em tempo real**: o funcionamento da célula descrito no relatório (o RCO espera pela resposta, com timeout e novas tentativas).

## Teste de comunicação (modo por lotes) — começar por aqui

```
 DIA 1 — tu            DIA 2 — parceiro                     DIA 3 — tu
 rco_enviar.py   ──►   parceiro_ia.py --lote          ──►   rco_receber.py
 (envia e sai)         (processa o que houver e sai)        (recolhe, valida, converte para o braço)
```

As mensagens ficam guardadas no CloudAMQP entre cada passo; não expiram.

**1. Enviar as imagens (tu):**
```
python rco_enviar.py imagens/                 # uma pasta ou vários ficheiros
```
Cada pedido fica registado em `pendentes/`, para validar a resposta quando chegar. Corre sempre os teus comandos **na mesma pasta**, para o `rco_receber.py` encontrar estes registos.

**2. Analisar (parceiro), quando quiser:**
```
python parceiro_ia.py --lote --sair-quando-vazio --guardar recebidas
```
Processa todas as imagens pendentes, envia um JSON por imagem e termina. Sem `--sair-quando-vazio`, fica à escuta.

**3. Recolher os resultados (tu), quando quiseres:**
```
python rco_receber.py                          # recolhe o que houver e termina
python rco_receber.py --continuo               # fica à escuta
```
Para cada resposta, em `resultados/`:
- `<image_id>_<request_id>.json` — resposta da IA (pontos em píxeis); com sufixo `_INVALIDA`, `_ERRO` ou `_DESCONHECIDA` se não passou;
- `<image_id>_<request_id>_robot.json` — os mesmos pontos em **coordenadas do braço** (metros, `robot_b_base`);
- `<image_id>_<request_id>_anotada.png` — a imagem com os pontos desenhados.

No fim mostra um resumo e os pedidos que ainda não têm resposta. Pode ser corrido quantas vezes for preciso.

### Conversão para coordenadas do braço

`rco_comm/coordenadas.py` faz a cadeia da secção 13 do relatório:

```
P_camera = Z · K⁻¹ · [u, v, 1]        (intrínsecos da câmara, distância Z ao plano da planta)
P_robot  = T_robot_camera · P_camera
```

Os parâmetros estão em `calibracao_exemplo.json`. **Os valores são fictícios** (câmara a 0.80 m a olhar para baixo, plano a 0.50 m), só para testar a cadeia. Para o braço real é preciso:
- calibrar os intrínsecos da câmara (`fx`, `fy`, `cx`, `cy`), por exemplo com um tabuleiro de xadrez e o OpenCV;
- medir ou calibrar a pose da câmara no referencial do braço (`T_robot_camera`, calibração mão-olho);
- definir a distância `plane_distance_m`. Assumir uma distância constante é uma aproximação: com uma câmara de profundidade (ou se a IA devolver `position` 3D), usa-se a profundidade real de cada ponto.

Se a resposta trouxer `position` 3D, essa posição é usada diretamente em vez do píxel.

## Princípios do relatório que a camada garante

- **A IA não controla os robôs.** Devolve apenas pontos de intervenção; o RCO valida e decide.
- **Transporte desacoplado.** O RCO usa a interface `IAIClient`. Há duas implementações: `MockAIClient` (sem rede, Fase 1) e `RabbitMQAIClient`. Um cliente REST ou ROS 2 futuro implementa a mesma interface sem mexer na máquina de estados do RCO.
- **Nenhuma resposta é aceite sem validação** (secção 12).
- **Uma falha nunca é tratada como sucesso.** Timeout, erro e resposta inválida têm estados próprios.

## Máquina de estados do pedido (secção 21)

```
READY → REQUEST_CREATED → REQUEST_SENT → WAITING_RESPONSE → RESPONSE_RECEIVED → VALIDATING → VALID → COMPLETED
                                                                                            └→ INVALID   (final)
REQUEST_SENT / WAITING_RESPONSE → TIMEOUT | AI_ERROR → nova tentativa (REQUEST_SENT) … → FAILED (final)
```

- **COMPLETED**: resposta válida; o RCO pode criar a `INTERVENTION_TASK` do Robot B.
- **INVALID**: a resposta chegou mas não passou na validação. Não há nova tentativa automática (a mesma imagem daria o mesmo resultado); o RCO decide se captura uma nova imagem.
- **FAILED**: timeouts ou erros de comunicação em todas as tentativas, ou erro definitivo da IA.

Eventos emitidos para o RCO (secção 15): `AI_ANALYSIS_REQUESTED`, `AI_ANALYSIS_RECEIVED`, `AI_ANALYSIS_VALIDATED`, `AI_ANALYSIS_INVALID`, `AI_ANALYSIS_TIMEOUT`, `ERROR_OCCURRED`, `AI_STALE_RESPONSE_DISCARDED`.

## Contrato de mensagens v1.0

Mensagens JSON UTF-8. Exemplos completos em `exemplos/`.

### Pedido — `AI_ANALYSIS_REQUEST` (RCO → IA)

- `schema_version`, `message_type`, `message_id`, `timestamp`
- `request_id` — identificador do pedido (ex. `AI_REQUEST-D14C486C200F`); igual em todas as tentativas
- `attempt` — número da tentativa (1, 2, …)
- `object_id` — ex. `PLANT-000123`
- `image` — `image_id`, `camera_id`, `encoding` (`jpeg`/`png`), `width`, `height`, `sha256`, `data_base64` (a imagem)
- `object_context` — `object_type` (`plant`), `frame_id` (`intervention_camera`)
- `deadline` — até quando o RCO espera a resposta

### Resposta — `AI_ANALYSIS_RESPONSE` (IA → RCO)

- `request_id`, `object_id`, `image_id` — iguais aos do pedido
- `analysis_id` — identificador da análise (ex. `ANALYSIS-E48EA0614023`)
- `status` — `ok` ou `error`
- `intervention_type` — `cut`
- `confidence` — confiança global, 0–1
- `frame_id` — `intervention_camera`
- `image_size` — `width`, `height`
- `points[]`:
  - `id` (inteiro), `type` (`cut_node`), `confidence`
  - `pixel` — `u` (coluna), `v` (linha), origem no canto superior esquerdo da imagem
  - opcionais, se a IA tiver profundidade: `position` (`x`, `y`, `z` em metros no `frame_id`) e `orientation` (`qx`, `qy`, `qz`, `qw`)
  - cada ponto tem `pixel` ou `position` (ou ambos)
- `model` — `name`, `version`; `processing_ms`; `timestamp`
- Se `status` = `error`: `error` com `code`, `message`, `retryable`
  - `INVALID_IMAGE`, `INVALID_REQUEST`, `MODEL_OUTPUT_INVALID` → definitivos (`retryable: false`)
  - `MODEL_FAILURE` → temporário (`retryable: true`), o RCO repete

### Validação feita pelo RCO (secção 12)

`request_id`, `object_id` e `image_id` correspondem ao pedido · `analysis_id` presente · `intervention_type` permitido · `frame_id` conhecido · confiança global ≥ mínimo (0.70 por defeito) · número de pontos entre 1 e 10 · tipo e confiança de cada ponto (≥ 0.50) · píxeis dentro da imagem · posições 3D dentro do espaço de trabalho · quaterniões normalizados · ids de pontos únicos.

As condições da célula (**Robot B disponível**, **zona segura**) são verificações do RCO, passadas em `extra_checks` (ver "Integração no RCO").

A conversão de píxeis/`intervention_camera` para o frame do Robot B (secção 13) é responsabilidade do módulo de coordenadas do RCO, não desta camada.

## Topologia RabbitMQ

- exchange `rco.ai` (direct, durável)
  - routing key `analysis.request` → fila `rco.ai.requests` (lida pelo parceiro)
  - routing key `analysis.response.<cell_id>` → fila `rco.ai.responses.<cell_id>` (lida pelo RCO)
  - routing key `batch.request` → fila `rco.ai.batch.requests` (modo por lotes, sem expiração)
  - routing key `batch.response.<cell_id>` → fila `rco.ai.batch.responses.<cell_id>` (modo por lotes, sem expiração)
- exchange `rco.ai.dlx` → fila `rco.ai.dead` (pedidos expirados ou ilegíveis)

Comportamentos importantes:
- Cada pedido expira no broker ao fim do timeout do RCO. Se o parceiro estiver desligado, o pedido vai para `rco.ai.dead` em vez de ser analisado mais tarde, quando a planta já não está na zona de intervenção.
- Respostas que chegam depois do timeout são descartadas pelo RCO (evento `AI_STALE_RESPONSE_DISCARDED`) e nunca são confundidas com a resposta do pedido atual.
- O parceiro só confirma (`ack`) o pedido depois de publicar a resposta. Se cair a meio, o pedido volta à fila.
- O modelo corre numa thread separada, para a ligação não cair durante inferências longas.
- Os dois lados religam-se sozinhos se a ligação cair.

## Instalação (as duas máquinas)

Python 3.9+:
```
pip install -r requirements.txt
```

Definir o URL do broker:
- Ubuntu/macOS: `export CLOUDAMQP_URL='amqps://UTILIZADOR:PASSWORD@cow.rmq2.cloudamqp.com/VHOST'`
- Windows PowerShell: `$env:CLOUDAMQP_URL = "amqps://UTILIZADOR:PASSWORD@cow.rmq2.cloudamqp.com/VHOST"`

Usar sempre `amqps://` (TLS). O URL contém a password: enviar ao parceiro por um canal privado e nunca o colocar no código.

## Modo em tempo real — lado RCO (a nossa máquina)

```
python rco_analisar.py imagem.jpg --object-id PLANT-000123
python rco_analisar.py imagem.jpg --object-id PLANT-000123 --image-id IMG-00891 --timeout 30 --tentativas 3
python rco_analisar.py imagem.jpg --mock        # sem broker nem parceiro
```

Mostra os eventos e o percurso de estados, e grava a resposta em `resultados/`. Código de saída: `0` COMPLETED, `2` INVALID, `3` FAILED.

Ver os pontos sobre a imagem:
```
python visualizar.py imagem.jpg resultados/IMG-00891_AI_REQUEST-XXXX.json
```

## Modo em tempo real — lado do parceiro (máquina da IA)

```
python parceiro_ia.py                          # IA simulada
python parceiro_ia.py --guardar recebidas      # guarda as imagens recebidas e as respostas
```

Para ligar o modelo real, substituir a função `analisar(imagem, pedido)` em `parceiro_ia.py`. Recebe os bytes da imagem e devolve um dict:

```python
{
    "intervention_type": "cut",
    "confidence": 0.94,
    "points": [
        {"id": 1, "type": "cut_node", "pixel": {"u": 423, "v": 218}, "confidence": 0.96},
        {"id": 2, "type": "cut_node", "pixel": {"u": 510, "v": 276}, "confidence": 0.91},
    ],
    "model": {"name": "modelo-parceiro", "version": "1.0"},
}
```

O resto (receber, descodificar, verificar integridade, responder, tratar erros) é feito pelo `AIServiceWorker`. A saída do modelo é verificada contra o contrato antes de ser enviada.

Modos de teste para o RCO: `--atraso 5` (inferência lenta), `--falha erro | invalido | silencio | aleatoria`.

## Integração no RCO

```python
from rco_comm import AIRequest, ImagePayload, ValidationRules
from rco_comm.rabbitmq import RabbitMQAIClient

client = RabbitMQAIClient(url, cell_id="cell_01", timeout_s=30, max_attempts=3,
                          rules=ValidationRules(min_confidence=0.8),
                          extra_checks=[lambda r: None if robot_b.available() else "Robot B indisponível",
                                        lambda r: None if safety.zone_safe() else "zona não segura"],
                          on_event=event_manager.publish)

req = AIRequest(object_id=ctx.object_id,
                image=ImagePayload(image_id, "intervention_camera_01", "jpeg", jpeg_bytes, 1024, 768))
res = client.analyze(req)          # bloqueia até COMPLETED / INVALID / FAILED

if res.ok:
    pontos = res.response.points   # -> CoordinateTransformer -> INTERVENTION_TASK (Robot B)
else:
    ...                            # res.state, res.errors -> tratamento de erros do RCO
```

Na Fase 1, `RabbitMQAIClient` troca-se por `MockAIClient(faults=["timeout", "ok"])`, que simula falhas sem rede.

## Testes

```
pytest tests/test_unit.py        # contrato, validação, máquina de estados (sem rede)
AMQP_TEST_URL=amqp://guest:guest@localhost:5672/%2F pytest tests/test_rabbitmq.py
```

Os testes de integração precisam de um RabbitMQ **de teste** (local ou Docker: `docker run -p 5672:5672 rabbitmq:4`). Não correr contra o CloudAMQP de produção, porque apagam as filas `rco.ai.*`.

`tests/test_lotes.py` cobre o modo por lotes (envio com o parceiro desligado, parceiro processa e sai, receção dias depois, resposta inválida, resposta sem registo local) e a conversão de coordenadas.

Cobertura do modo em tempo real: fluxo completo · 5 pedidos seguidos · parceiro desligado (timeout, nova tentativa, pedidos expiram para a dead letter queue) · resposta atrasada descartada · inferência de 7 s com heartbeat de 2 s · pedidos mal formados · queda de ligação do RCO e do parceiro.

## Estrutura

```
rco_comm/
  messages.py      contrato: AIRequest, AIResponse, InterventionPoint
  validation.py    validação da resposta (secção 12)
  client.py        IAIClient, máquina de estados, retries, MockAIClient
  rabbitmq.py      RabbitMQAIClient (RCO) e AIServiceWorker (parceiro)
  service.py       adaptação do analisador do parceiro ao contrato
  simulated_ai.py  IA simulada (pontos de corte no caule)
  batch.py         modo por lotes: BatchSender, BatchReceiver, registo de pendentes
  coordenadas.py   píxeis -> câmara -> braço (calibração)
rco_enviar.py      modo por lotes: enviar imagens
rco_receber.py     modo por lotes: recolher, validar e converter para o braço
calibracao_exemplo.json  calibração fictícia para testes
rco_analisar.py    CLI do lado RCO (tempo real)
parceiro_ia.py     CLI do lado do parceiro (ponto de integração do modelo)
visualizar.py      desenha os pontos sobre a imagem
exemplos/          pedido, resposta, imagem e imagem anotada
tests/             testes unitários e de integração
```
