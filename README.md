# Technical Test — Koin

Pipeline de ETL desenvolvido em Python para processar e transformar dados seguindo a arquitetura de camadas **Bronze → Silver → Gold**.

---

## 🗂️ Estrutura do Projeto

```
technical_test_koin/
├── data/               # Dados de entrada e saída
├── models/             # Modelos e schemas
├── src/                # Módulos do pipeline (extract, transform, load)
├── etl_main.py         # Script principal — camada Bronze
├── silver.py           # Script de transformação — camada Silver
├── gold.py             # Script de agregação — camada Gold (saída final)
├── pyproject.toml
└── uv.lock
```

---

## ⚙️ Pré-requisitos

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (gerenciador de pacotes e ambientes virtuais)

### Instalação das dependências

```bash
uv sync
```

---

## 🚀 Como executar

Os scripts devem ser executados **na ordem abaixo**. Cada etapa depende da saída da anterior.

### 1. Bronze — Extração e carga inicial

```bash
uv run python etl_main.py
```

Responsável por realizar o ETL dos dados brutos dos schemas `customers` e `orders`, aplicar transformações básicas e carregá-los na camada Bronze.

---

### 2. Silver — Limpeza e enriquecimento

```bash
uv run python models/silver.py
```

Processa os dados da camada Bronze, aplica limpeza, padronização e regras de negócio, gerando os dados da camada Silver.

> ⚠️ **Este script precisa ser executado antes do `gold.py`.**

---

### 3. Gold — Agregação e resultado final

```bash
uv run python models/gold.py
```

Consolida e agrega os dados da camada Silver, produzindo o datamart e também o **arquivo final do teste**.

> ✅ **A saída do `gold.py` (datamart_transactions.csv) é o entregável final deste teste técnico.**

---

## 🔄 Fluxo do Pipeline

```
Fonte de dados
     │
     ▼
 etl_main.py   →   Camada Bronze   (dados brutos extraídos)
     │
     ▼
  silver.py    →   Camada Silver   (dados limpos e transformados)
     │
     ▼
   gold.py     →   Camada Gold     ✅ ARQUIVO FINAL DO TESTE
```