# Testes Unitários do registro-vert

## Estratégia

Os testes validam **as confs JSON em isolamento**, sem depender de Kafka real
ou tabelas Delta de produção:

1. **Fixtures CSV** em `fixtures/` simulam as tabelas Delta (silver_cessao,
   bronze_remessa, etc).
2. O helper `register_fixtures(["silver_cessao", ...])` lê cada CSV e registra
   como temp view com nome qualificado (`lastros.silver_cessao`).
3. O helper `run_conf(path, columns={...})` carrega a conf de produção,
   troca todos os `format: delta` por `format: view` (resolve para as temp
   views registradas pelas fixtures), descarta outputs Kafka/Delta reais e
   roda via `fw.run_from_dict`.
4. Asserts validam o resultado lendo a view de saída e comparando campos
   específicos (tipos, valores, schema do struct payload).

## Estrutura

```
tests_unit/
├── conftest.py                       # fixtures pytest + helpers
├── fixtures/                         # CSVs mock das tabelas Delta
│   ├── silver_cessao.csv
│   ├── silver_cessoes_status.csv
│   ├── silver_parametrizacao_registro_lastro.csv
│   ├── silver_contrato.csv
│   ├── silver_status_criterios_contratos.csv
│   ├── bronze_remessa.csv
│   └── silver_controle_registro_cessoes.csv
├── gabaritos/                        # outputs esperados (JSON; vazio por agora)
├── test_framework_basico.py          # valida features framework usadas
└── test_nota_comercial_b3.py         # valida fluxo NC end-to-end (sem Kafka)
```

## Rodando

```bash
pip install -e .
pip install pytest pyspark
cd tests/registro_vert
pytest tests_unit/ -v
```

## Adicionando testes para outros ativos

Para CCB, CPR e Duplicata, adicione:
1. Fixtures CSV específicas em `fixtures/`:
   - CCB: silver_sacado, silver_dados_auxiliares, silver_emissao_ccb, silver_parcela
   - CPR: bronze_remessa enriquecida + silver_dados_auxiliares
   - Duplicata: bronze_remessa enriquecida + silver_lastros_relacionamento + silver_parcela
2. Crie `test_<ativo>.py` seguindo o padrão de `test_nota_comercial_b3.py`:
   - Rode `cessoes_base.json` + `<ativo>/cessoes_pendentes.json` (apenas a view)
   - Assert no schema do payload struct
   - Assert nos valores chave (campos fixos, regras condicionais)

## Limitações conhecidas

- Os testes NÃO validam escritas Kafka/Delta reais — apenas a view de payload.
- Para validar Kafka/Delta, seria necessário um Kafka embedded e Delta Lake
  no classpath. Não está nesse PR.
- Gabaritos JSON (`gabaritos/`) ainda não são usados; a função
  `assert_view_matches_gabarito` está pronta para uso futuro.
