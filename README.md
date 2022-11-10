# cited-referentes

## Descrição
Este repositório contêm códigos para realizar duas tarefas: a) criar bases de correção e b) enriquecer citações. Enriquecer uma citação significa descobrir a qual código ISSN o título de periódico, ano e volume citados se referiram. E para isso, é utilizaddo como apoio um conjunto de arquivos CSV (bases de correção) que contêm dados de periódicos de todo o mundo. Essas bases são compostas pelos diferentes títulos, códigos ISSN, anos e volumes existentes.

## Instalação
Para instalar a aplicação, basta criar um ambiente virtual Python e adicionar as dependências, como descrito a seguir:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Scripts para geração de bases de correção de títulos/ISSNs
O diretório https://github.com/scieloorg/cited-references/tree/master/script concentra os scripts necessários para a criação de bases de correção.

## Script para enriquecimento de citações
O principal script para enriquecimento de citações está disponível em https://github.com/scieloorg/cited-references/blob/master/core/matchers/enrich_references.py
