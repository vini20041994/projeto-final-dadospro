# Relatório — datalake de tracking da DadosPro

> **Como ler os números.** Todos vêm das tabelas Gold em `s3a://datalake-tracking/gold/`, geradas por `python pipeline/rodar_tudo.py` sobre a landing de 01/09 a 14/09/2026 (14 dias; 8.793 sessões, 5.994 visitantes, 573 compras). Receita em reais, datas em UTC. Os dados são sintéticos (gerador do projeto) e cobrem só 14 dias, então diferenças pequenas entre grupos podem ser ruído; onde a amostra é pequena, o texto avisa. Não há custo de mídia nos dados: nenhuma conclusão abaixo calcula retorno sobre investimento.

## 1. Funil

**Pergunta:** em que etapa a jornada de compra perde mais gente, por plano e por canal?
**Tabelas:** `gold/funil_compra`, `gold/funil_lead`.

**Resposta curta:** a maior perda está logo no começo, entre ver a tabela de planos e escolher um plano. Depois que a pessoa escolhe, o funil é parecido entre os três planos; o que muda muito é o canal.

Funil total (sessões que tiveram cada evento em algum momento, sem exigir as etapas anteriores, conforme 5.4):

| Etapa | Sessões | Sobre a etapa anterior | Sobre o topo |
| --- | ---: | ---: | ---: |
| `view_item_list` (viram os planos) | 5.258 | n/a | 100% |
| `select_item` (escolheram um plano) | 1.719 | 32,7% | 32,7% |
| `add_to_cart` | 1.291 | 75,1% | 24,6% |
| `view_cart` | 1.255 | 97,2% | 23,9% |
| `begin_checkout` | 912 | 72,7% | 17,3% |
| `add_payment_info` | 684 | 75,0% | 13,0% |
| `purchase` | 573 | 83,8% | 10,9% |

- **Maior vazamento:** `view_item_list` → `select_item`, onde 67% das sessões que viram os planos não escolhem nenhum. É a única etapa abaixo de 70%. As outras perdas ficam entre 16% e 27%, e a de `view_cart` é de só 3%.
- **Por plano** (`select_item` → `purchase`): Pro 32,9% (264 de 803), Básico 32,8% (195 de 595), Premium 32,1% (103 de 321). Não há diferença relevante entre planos depois da escolha. O Pro é o mais escolhido (803 contra 595 e 321).
- **Por canal** (a diferença está no topo):

| Canal | Viram os planos | Escolhem plano | Compram | Compra sobre o topo |
| --- | ---: | ---: | ---: | ---: |
| newsletter / email | 335 | 57,3% | 74 | 22,1% |
| google / cpc | 1.554 | 40,1% | 221 | 14,2% |
| direct / (none) | 1.008 | 33,5% | 112 | 11,1% |
| google / organic | 1.090 | 31,4% | 101 | 9,3% |
| instagram / paid_social | 1.271 | 17,6% | 65 | 5,1% |

  Entre quem **escolhe** um plano, a compra é parecida em todos os canais (29% a 39%). O Instagram perde porque poucos escolhem (17,6%), não porque desistem depois.
- **`view_item`** (abrir o detalhe do plano) fica fora da sequência, como pede o enunciado: 985 sessões, ou 57,3% das sessões com `select_item`.
- **Leitura das linhas por plano:** em `funil_compra`, as conversões por plano podem passar de 100% (por exemplo 101,5% do Pro de `view_item_list` para `select_item`). Isso acontece porque uma sessão conta numa etapa sem exigir as anteriores (5.4, item 4): há sessões que escolhem um plano sem passar por `view_item_list`. A linha `(nenhum)` reúne 11 compras de sessões sem `select_item`.

**Funil de lead** (`funil_lead`, só a landing `/`): 8.382 sessões com `page_view` → 5.892 rolaram ao menos 50% (70,3%) → 522 deixaram o e-mail (8,9% de quem rolou; 6,2% de todas).
Entre quem rolou, Google orgânico (9,8%), Google CPC (9,4%) e Instagram (9,2%) ficam em patamar parecido; o direto fica em 8,3% e a newsletter em 3,8%, mas com só 14 leads, pouco para concluir algo.

**Recomendação:** concentrar o esforço na decisão de escolher o plano (tabela de planos, ordem dos cartões, chamada para o Pro, prova social ao lado) e testar a mudança medindo `select_item ÷ view_item_list`. No Instagram, a página para onde o anúncio leva deveria ir direto aos planos, já que quem escolhe plano compra quase tão bem quanto nos outros canais.

## 2. Engajamento

**Pergunta:** o vídeo de apresentação é visto até o fim? O que o visitante procura no FAQ e não encontra?
**Tabelas:** `gold/sessoes`, `gold/retencao_video`, `gold/buscas_faq`, `gold/saidas_downloads`.

**Vídeo:** das 2.890 sessões que deram play (34% das sessões na landing), **30,4% chegaram ao fim** (877). A retenção cai de forma contínua:

| Marco | Sessões | % de quem deu play |
| --- | ---: | ---: |
| 10% | 2.626 | 90,9% |
| 25% | 2.138 | 74,0% |
| 50% | 1.474 | 51,0% |
| 75% | 1.105 | 38,2% |
| 100% | 877 | 30,4% |

A maior queda está entre 25% e 50% (23 pontos percentuais). Metade do público sai antes da metade do vídeo.

**FAQ:** foram 303 buscas. **161 (53%) não tiveram resultado**, todas em cinco termos que o FAQ não cobre:

| Termo sem resposta | Buscas |
| --- | ---: |
| estagio | 44 |
| presencial | 33 |
| bolsa | 32 |
| power bi | 28 |
| linux | 24 |

Os termos com resposta (parcelar, acesso, certificado, reembolso, apostila) somam 142 buscas, todas com resultado.

**Outros sinais:**
- Nas 8.793 sessões, a mediana de duração é de 1 min 56 s (média 2 min 29 s). 29,6% rolaram até 90% da página.
- Saídas para redes sociais: 366 cliques (Instagram 102, LinkedIn 92, WhatsApp 87, YouTube 85). Download da ementa: 335.
- Conversão em compra por dispositivo: desktop 7,3%, mobile 6,1%, tablet 5,6%. Mobile é a maioria das sessões (60%), então a diferença de 1,2 ponto entre desktop e mobile vale um teste, mas não indica problema grave.

**Recomendação:** (1) acrescentar ao FAQ respostas sobre estágio, aulas presenciais, bolsa, Power BI e Linux, os temas que mais deixam o visitante sem resposta; (2) encurtar ou reordenar o vídeo, trazendo a oferta e os planos para antes do marco de 50%, onde metade do público já saiu.

## 3. Receita e aquisição

**Pergunta:** quanto entrou de fato, descontados os reembolsos, e qual canal traz cliente que não desiste?
**Tabelas:** `gold/receita_diaria`, `gold/taxa_reembolso`, `gold/adesao_order_bump`, `gold/lead_para_compra`.

**Receita (01/09 a 14/09, por data da compra):**

| Receita bruta | Reembolsada | **Líquida** | Pedidos | Ticket médio |
| ---: | ---: | ---: | ---: | ---: |
| R$ 338.523,10 | R$ 28.608,50 | **R$ 309.914,60** | 573 | R$ 590,79 |

O reembolso devolveu 8,45% do valor vendido; 48 dos 573 pedidos (8,4%) foram reembolsados. Por plano, a líquida é Pro R$ 145.795,65, Premium R$ 108.812,75 e Básico R$ 55.306,20.

**Qual canal traz cliente que não desiste:**

| Canal | Pedidos | Taxa de reembolso | Líquida | Líquida ÷ bruta |
| --- | ---: | ---: | ---: | ---: |
| direct / (none) | 112 | 1,8% | R$ 62.143,25 | 98,0% |
| newsletter / email | 74 | 2,7% | R$ 45.415,65 | 96,7% |
| google / cpc | 221 | 9,1% | R$ 119.631,15 | 91,8% |
| google / organic | 101 | 8,9% | R$ 54.787,70 | 91,5% |
| **instagram / paid_social** | 65 | **23,1%** | R$ 27.936,85 | **73,8%** |

- **O Instagram é o canal que mais desiste:** 15 dos 65 pedidos foram reembolsados (23,1%), quase três vezes a média. No Premium vindo do Instagram, são 6 de 15 pedidos (40%). Com 15 pedidos o número é instável, mas o padrão se repete nos três planos (Básico 19,2%, Pro 16,7%, Premium 40,0%): as três combinações com Instagram são as três maiores taxas de reembolso por plano × canal; a seguinte é 11,1%.
- **Direct e newsletter são os que menos desistem** (menos de 3%).
- O **Google CPC** é o maior em volume: 221 pedidos e 38,6% da receita líquida, com reembolso de 9,1%, na média.
- O Instagram responde por 24,2% das sessões, 11,3% dos pedidos e 9,0% da receita líquida.

**Order bump:** 90 das 573 compras levaram a Mentoria em grupo (15,7%). Por plano: Básico 15,0%, Premium 15,0%, Pro 16,5%; não há diferença relevante.

**Lead → compra:** dos 522 leads, 40 compraram depois (7,7%), com mediana de **4,6 dias** entre o lead e a primeira compra posterior.

| Canal do lead | Leads | Compraram | Taxa | Mediana (dias) |
| --- | ---: | ---: | ---: | ---: |
| google / cpc | 165 | 16 | 9,7% | 3,5 |
| direct / (none) | 94 | 8 | 8,5% | 7,5 |
| google / organic | 118 | 8 | 6,8% | 1,9 |
| instagram / paid_social | 131 | 7 | 5,3% | 6,1 |
| newsletter / email | 14 | 1 | 7,1% | 10,1 |

Cuidado ao ler: a janela tem só 14 dias, e leads dos últimos dias não tiveram tempo de comprar. A taxa tende a estar subestimada, e a mediana, enviesada para baixo. Com 40 compras no total, as diferenças entre canais são frágeis (newsletter tem 1 compra).

**Recomendação:** (1) investigar o Instagram antes de aumentar o investimento: ele traz pouca compra por sessão, devolve quase um quarto dos pedidos e a receita líquida por real vendido é a menor. Os dados não trazem o custo da mídia, então só dá para dizer que o retorno precisa ser comparado com o do Google CPC, que gera mais receita com reembolso na média; (2) usar a newsletter e o tráfego direto, que quase não geram reembolso, para ofertas de maior valor, como o Premium; (3) como leads demoram em média quase 5 dias para comprar, enviar um e-mail de acompanhamento entre o 2º e o 5º dia após o download da ementa.

## 4. Previsão

**Pergunta:** quais sessões estão perto de comprar, e que perfis de visitante existem?
**Tabelas:** `gold/propensao_sessao`, `gold/metricas_modelo`, `gold/segmentos_visitantes`.

### 4.1 Modelo de propensão

- **Modelo:** regressão logística. Rótulo: a sessão tem um `purchase`. As features usam só a landing (`/`) **antes do primeiro `add_to_cart`, `view_cart`, `begin_checkout`, `add_shipping_info`, `add_payment_info` ou `purchase`** da sessão: rolagem máxima, se iniciou o vídeo e até onde viu, se viu os planos, se escolheu um plano (e qual), se abriu o detalhe, buscas, downloads, cliques, total de eventos, canal e dispositivo. Nada do checkout entra, então não há vazamento.
- **Treino e teste por visitante** (hash do `user_pseudo_id`, ~20% no teste): 7.060 sessões no treino (467 compras) e 1.733 no teste (106 compras, 6,1%). Sessões do mesmo navegador não ficam dos dois lados.
- **Limiar:** 0,257, o que maximiza o F1 no treino.

Métricas no **conjunto de teste**, classe positiva (sessão com compra):

| AUC | F1 | Precisão | Recall |
| ---: | ---: | ---: | ---: |
| 0,935 | 0,491 | 0,338 | 0,896 |

Das 106 compras do teste, o modelo achou 95; apontou 186 sessões que não compraram e perdeu 11. Entre as 1.452 sessões abaixo do limiar, só 11 compraram (0,8%); entre as 281 acima, 95 compraram (33,8%). No teste, a probabilidade máxima prevista é 0,44: o modelo não tem sessão de confiança alta.

**Limite importante: o modelo quase só repete "escolheu um plano".** Das 573 compras, 562 vêm de sessões com `select_item`. Uma regra de uma linha (prever compra se a sessão escolheu um plano) dá, no mesmo conjunto de teste, AUC 0,918, precisão 0,331, recall 0,962 e F1 0,493. O modelo (AUC 0,935, F1 0,491) não é melhor em F1 do que essa regra. Entre as 308 sessões de teste que escolheram um plano, a AUC do modelo é de 0,54, perto do acaso: ele quase não distingue, dentro desse grupo, quem compra de quem não compra. As features comportamentais (rolagem, vídeo, buscas) trazem pouco além do sinal de escolher o plano.

Em termos práticos, o modelo serve para separar as sessões que **nem chegaram a escolher plano** (11 compras em 7.074 sessões, 0,2%) das que escolheram (cerca de 1 em 3 compra: 102 de 308 no teste). Ele não consegue dizer, entre quem escolheu, quem vai comprar.

### 4.2 Perfis de visitante

K-Means sobre 5.994 visitantes, com as variáveis padronizadas (sessões, eventos, rolagem máxima, vídeo, buscas, lead, chegou ao carrinho, comprou). O K foi escolhido pela silhueta entre 3 e 6: **K = 4** (silhueta 0,60; K=3: 0,47; K=5: 0,45; K=6: 0,51).

| Grupo | Visitantes | Perfil |
| --- | ---: | --- |
| 1 | 4.220 (70,4%) | **Curiosos de passagem**: 1,2 sessão, 7 eventos, rolagem média 56%, sem lead nem busca, 6,5% chegam ao carrinho, praticamente ninguém compra |
| 2 | 1.033 (17,2%) | **Compradores e carrinhos**: 2,2 sessões, 21 eventos, 88% chegam ao carrinho e 54% compram. Quase todas as compras estão aqui |
| 3 | 444 (7,4%) | **Leads**: todos deixaram o e-mail, rolagem média 78%, só 0,2% compraram |
| 4 | 297 (5,0%) | **Leitores do FAQ**: média de 1 busca por visitante, rolagem 86%, 14% viraram lead, 4% compraram |

Os grupos 2 e 3 são em parte definidos pelas próprias variáveis de lead e de compra, então descrevem o resultado do funil e não servem de previsão. Os grupos 1 e 4 são os mais informativos: o 4 mostra que quem usa o FAQ lê muito, mas compra pouco (4%), o que combina com o FAQ sem resposta para 53% das buscas (seção 2).

**Recomendação:** (1) não usar o modelo atual para decidir abordagem individual; ele só repete o sinal de escolha de plano. Para ganhar valor, precisa de features que não existem hoje, como o histórico entre sessões ou o tempo parado na tabela de planos; (2) tratar o grupo 3 (444 leads que não compraram) com o acompanhamento por e-mail sugerido na seção 3; (3) tratar o grupo 4 com respostas no FAQ.

### 4.3 Limites da análise

- Janela de 14 dias e dados sintéticos: as taxas podem não se repetir fora dela.
- Os dias 08, 09, 10 e 13/09 têm menos compras (28 a 34 por dia, contra 42 a 49 nos outros dias). Não investiguei a causa; não dá para dizer se é variação do gerador ou efeito de rejeições.
- As rejeições discutidas em 5.4 deixam de fora compras legítimas (31 `purchase` sem `transaction_id`), então a receita bruta tende a estar subestimada.

---

## 5. Qualidade do dado

> **Origem dos números:** as contagens foram feitas primeiro direto na landing (`s3a://datalake-tracking/landing/`, gerada em 20/10/2026) e depois **conferidas com a execução do job Silver** (`pipeline/rodar_tudo.py`): 64.372 linhas na Bronze, 656 duplicatas, 1.903 rejeitados e 61.813 válidos, com o mesmo total por motivo da tabela 5.3. Os detalhes de 5.4 e 5.5 (valores em USD, `value` negativo, 683 eventos em 2036 etc.) vêm da exploração da landing e não foram refeitos depois da Silver. Se o gerador for rodado de novo, os números mudam um pouco, mas as armadilhas continuam as mesmas.

### 5.1 Visão geral

| Etapa | Eventos |
| --- | ---: |
| Linhas na landing (= linhas na Bronze) | 64.372 |
| Descartadas como duplicata (R5) | 656 |
| Eventos únicos (após dedup) | 63.716 |
| Rejeitados pela validação (R7) | 1.903 |
| **Aprovados na Silver** | **61.813** |

Cerca de **3,0%** dos eventos únicos foram rejeitados. Nenhum evento caiu em mais de uma regra, então cada rejeitado tem exatamente um motivo.

### 5.2 Duplicatas

- **656 eventos** chegaram duas vezes, com o mesmo `event_id`. Nenhum chegou três vezes ou mais.
- As duas cópias são **idênticas** byte a byte. Ficam sempre na **mesma pasta de dia**, mas em **arquivos de lote diferentes** (ex.: `coletor-s42-001.jsonl` e `coletor-s42-002.jsonl`). É o reenvio de lote que o contrato avisa.
- Há duplicatas em quase todos os tipos de evento, inclusive **5 `purchase`** e **1 `refund`**. Sem a dedup, a receita bruta e a reembolsada ficariam infladas.
- **Tratamento:** a Silver deduplica por `event_id` **antes** da validação. Assim, cada evento é validado e contado uma vez só.

### 5.3 Rejeitados por motivo

| Motivo (R7) | Eventos | O que foi encontrado |
| --- | ---: | --- |
| evento fora do catálogo | 271 | `add_to_wishlist`, um evento que não está no contrato. Vem com `params` e `items` vazios, de 257 visitantes |
| timestamp ausente ou futuro | 948 | 273 linhas com `event_ts` nulo e 683 com data em **2036** (contagens antes da dedup) |
| transação sem id | 31 | `purchase` com `transaction_id: null` |
| valor monetário inválido | 349 | 276 linhas com `value` negativo e 77 com `currency = "USD"` (antes da dedup) |
| marco fora do padrão | 272 | `scroll` com `percent_scrolled = 33`. O vídeo está limpo: só 0, 10, 25, 50, 75 e 100 |
| reembolso sem compra | 32 | `refund` cujo `transaction_id` não existe em nenhum `purchase` válido |
| **Total** | **1.903** | |

### 5.4 Alguma regra rejeitou dado legítimo?

**Sim.** As regras do enunciado foram aplicadas como estão escritas, para os números serem comparáveis. Mesmo assim, há indícios fortes de que parte dos rejeitados é dado verdadeiro com um defeito de formato:

| Caso | Eventos | Por que parece legítimo | Impacto de rejeitar |
| --- | ---: | --- | --- |
| `generate_lead` em USD com `value = 0` | 28 | Um valor zero não depende de moeda. O lead aconteceu | ~5% dos leads somem de `funil_lead` e `lead_para_compra` |
| Eventos do funil em USD | 49 | Os valores são **idênticos aos preços em reais** (297, 597, 997, 147…). Só a etiqueta está errada | Etapas do funil ficam um pouco menores |
| `value` negativo no funil | 275 | O `items[].price` do mesmo evento é positivo. É só o sinal trocado | `funil_compra` perde, por exemplo, 79 `add_to_cart` e 63 `view_cart` |
| `refund` com `value = -282,15` | 1 | Existe a compra correspondente, de +282,15, do mesmo visitante | A receita reembolsada fica R$ 282,15 menor |
| `event_ts` 10 anos no futuro | 683 | Deslocamento de **exatamente +3.650 dias** em todos. O resto do evento é coerente. Parece um relógio de navegador ou de lote errado | Abre "buracos" nas sessões de 620 visitantes, que também têm 8.651 eventos normais |
| `purchase` sem `transaction_id` | 31 | Valor, plano e forma de pagamento coerentes com a tabela de preços | Essas vendas ficam fora da receita bruta |

Outro caso parece **corretamente** rejeitado: dos 32 reembolsos sem compra, 31 têm `value = 597` e `items` vazio. Também não têm relação com as compras sem `transaction_id`: só 1 visitante coincide.

### 5.5 Armadilhas e como foram tratadas

1. **Reenvio de lote (duplicatas).** 656 cópias idênticas em arquivos de lote diferentes. Tratamento: dedup por `event_id` na Silver, antes de validar. A Bronze guarda as duas cópias (append-only), e a coluna de arquivo de origem mostra de qual lote veio cada uma.
2. **Relógio 10 anos adiantado.** 683 eventos em 2036, concentrados em geral num único lote por dia (ex.: `coletor-s42-003` de 03/09). Tratamento: rejeitados como "timestamp ausente ou futuro", comparando com o instante de ingestão. Não foram corrigidos (−3.650 dias) para não inventar dado. A correção fica registrada como possível melhoria.
3. **Sinal trocado no `value`.** 276 eventos com valor negativo, 1 deles um `refund`. Tratamento: rejeitados como "valor monetário inválido". A Bronze guarda o valor original, que pode ser recuperado depois.
4. **Moeda errada.** 77 eventos em USD com valores em reais, 28 deles leads com valor 0. Tratamento: rejeitados como "valor monetário inválido", com o impacto nos leads registrado na seção 5.4.
5. **Evento fora do contrato.** 274 `add_to_wishlist` (271 únicos). Tratamento: rejeitados como "evento fora do catálogo". A lista de eventos válidos é a da tabela do contrato, com **22 nomes**, embora o texto fale em "20 eventos".
6. **Marco de rolagem inexistente.** 276 `scroll` em 33% (272 únicos). Tratamento: rejeitados como "marco fora do padrão".
7. **Compras sem id e reembolsos órfãos.** 31 `purchase` sem `transaction_id` e 32 `refund` sem compra. Tratamento: primeiro valida as compras; depois o `refund` é conferido **só contra as compras válidas**, como pede a regra.
8. **`session_hint` não confiável.** Muda a cada recarga de página. Tratamento: ignorado. As sessões são calculadas com a regra de 30 minutos por `user_pseudo_id` (R8). A coluna é mantida na Bronze por fidelidade.
9. **Campos fora do contrato em `items`.** Os itens de `view_item_list` e `select_item` trazem `index`, `item_list_id` e `item_list_name`, que o contrato não cita. Tratamento: incluídos no schema explícito da Bronze, para nada se perder.
10. **Sessões na virada do dia.** O dia 15/09 tem só 6 eventos, de sessões que começaram em 14/09. Tratamento: a partição da Bronze usa o dia de **coleta** (do caminho da landing), e as datas de negócio usam o `event_ts` em UTC (5.4, item 10).

### 5.6 O que está limpo

Também verificado, sem problema encontrado:
- JSON sempre bem formado; nenhuma linha corrompida.
- O `event_name` do caminho é sempre igual ao de dentro do evento.
- `event_ts`, quando preenchido, sempre em ISO-8601 com `Z`.
- Números sempre como número JSON, nunca como texto.
- Canais (`source / medium`), dispositivos e domínio das páginas sem variações de grafia.
- `user_id` sempre no formato `u-xxxxxxxx`; nenhum navegador com dois `user_id`.
- Toda `purchase` tem exatamente 1 plano no `items`; 93 têm também o order bump na landing bruta (90 na Silver, depois da dedup e da validação).
