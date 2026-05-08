# Technical Test Koin — Pipeline ETL

Um pipeline ETL (Extract, Transform, Load) em Python que processa dados de `customers` (clientes) e `orders` (pedidos) em três etapas sequenciais: extração, transformação e carregamento.

---

## Estrutura do Projeto

```
technical_test_koin/
├── data/               # Arquivos de dados de entrada
├── models/             # Modelos de dados
├── src/
│   ├── extract_data.py     # Lógica de extração
│   ├── transform_data.py   # Lógica de transformação
│   └── load_data.py        # Lógica de carregamento
├── etl_main.py         # Ponto de entrada do pipeline
├── pyproject.toml      # Metadados e dependências do projeto
└── uv.lock             # Versões travadas das dependências
```

---

## Pré-requisitos

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** — gerenciador de pacotes utilizado pelo projeto

Para instalar o `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Ou via pip:

```bash
pip install uv
```

---

## Instalação

**1. Clone o repositório**

```bash
git clone https://github.com/souzasantos-yuri/technical_test_koin.git
cd technical_test_koin
```

**2. Instale as dependências**

```bash
uv sync
```

Isso criará um ambiente virtual e instalará todas as dependências (incluindo o `pandas`) conforme definido no `uv.lock`.

---

## Executando o Pipeline

```bash
uv run etl_main.py
```

O pipeline será executado para os schemas `customers` e `orders` sequencialmente, registrando o progresso de cada etapa:

```
2025-01-01 00:00:00 - INFO - First stage: Extracting <customers>
2025-01-01 00:00:00 - INFO - Rows extracted: 100
2025-01-01 00:00:00 - INFO - Second stage: Transforming <customers>
2025-01-01 00:00:00 - INFO - Rows removed: 5
2025-01-01 00:00:00 - INFO - Last stage: Loading <customers>
2025-01-01 00:00:00 - INFO - Pipeline for <customers> fully completed!
2025-01-01 00:00:00 - INFO - Saved rows: 95
```

---

## Como Funciona

A função `pipeline()` em `etl_main.py` executa três etapas para cada schema:

1. **Extração** — lê os dados brutos via `extract_data(schema)`
2. **Transformação** — limpa e processa os dados via `data_transformation(df, schema)`, removendo linhas inválidas
3. **Carregamento** — persiste o resultado via `load_data(df, schema)`

Erros em qualquer etapa são capturados, registrados em log e o traceback completo é exibido sem interromper o pipeline para os demais schemas.

---

## Principais decisões tomadas

1. Dados persistidos salvos em arquivo ao invés de transportar via memória
2. Separação clara dos arquivos em pastas
3. Utilização da arquitetura Medalhão para melhor entendimento das camadas de dados
4. Utilização do pandas para fazer o ETL
5. Finalização com o modelo dimensional Star Schema, possuindo tabelas dimensão e tabelas fato, unificando em um datamart analítico.
6. Utilização do pacote UV para facilitar a criação do ambiente.
