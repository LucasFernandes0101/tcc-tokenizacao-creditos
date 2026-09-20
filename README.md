# Tokenização de créditos: artefato reproduzível

Artefato complementar ao TCC **Tokenização de créditos em ecossistemas digitais**, de Lucas Fernandes Cassetari, MBA em Engenharia de Software, USP/Esalq.

Este repositório contém um demonstrador independente em Python e SQLite, verificações determinísticas e arquivos de rastreabilidade usados na aplicação da matriz de decisão arquitetural ao caso público do OpenMeter.

> O demonstrador não é código do OpenMeter, não executa o produto e não representa uma implementação de produção. Ele reproduz apenas um recorte controlado das regras analisadas no TCC.

## Como reproduzir

Requisitos: Python 3.10+ com `sqlite3`. Não há dependências externas.

```bash
python run_tests.py results_reproduzidos
```

A execução de referência registrada no TCC produziu **103 verificações aprovadas**: 28 verificações dirigidas e 75 combinações de fronteira.

Os valores usados nos testes são entradas controladas. Eles não representam tráfego, SLAs, custos, clientes ou medições do OpenMeter.

## Estrutura

```text
.
├── ledger.py
├── run_tests.py
├── evidencias/
│   ├── cobertura_requisitos.csv
│   ├── rastreabilidade.csv
│   ├── sources.json
│   └── recorte_openmeter.json
├── results/
│   ├── results.json
│   └── results.csv
├── CITATION.cff
└── LICENSE
```

- `ledger.py`: implementação reduzida e independente do ledger de créditos.
- `run_tests.py`: entradas, oráculos e verificações executáveis.
- `evidencias/cobertura_requisitos.csv`: mapeamento entre requisitos R1-R6 e verificações.
- `evidencias/rastreabilidade.csv`: relação entre afirmações, fontes, evidências e limites.
- `evidencias/sources.json`: fontes públicas utilizadas no recorte técnico.
- `evidencias/recorte_openmeter.json`: commit e arquivos públicos do OpenMeter inspecionados.
- `results/`: resultados preservados da execução de referência.

## Escopo

O demonstrador preserva concessões de crédito, prioridade, vigência, expiração, anulação, rollover, excedentes, idempotência por `source/id`, transações e reconstrução do estado.

Não foram avaliados recorrência de calendário, reordenação de eventos, autenticação, pagamentos, tributos, consenso distribuído, tolerância a partições ou desempenho de produção.

O commit público do OpenMeter utilizado no recorte foi:

`6d76d8a6fa90fbbab2d41035d31df2acec7ad3af`

## Resultado principal

O contraexemplo C01 mostra por que um saldo agregado isolado pode perder informação: duas concessões podem ter o mesmo total inicial e produzir direitos disponíveis diferentes posteriormente quando possuem vencimentos distintos.

Isso sustenta, dentro do escopo testado, a necessidade de preservar estado por concessão quando prioridade, vigência e expiração fazem parte da regra de negócio.

## Licença

O código original deste demonstrador está sob licença MIT. Projetos, documentação e marcas de terceiros permanecem sujeitos aos direitos de seus respectivos autores.
