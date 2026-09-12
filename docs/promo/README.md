# Decks promocionais

## Studio

`sparquet-studio.pdf` — 12 páginas em 1080×1350 (4:5, o formato que o LinkedIn
mostra maior no feed). Fonte: `promo.html`.

As imagens (`*.png`) não ficam versionadas — são capturas de tela e se refazem em
um comando. Para regerar o deck inteiro:

```bash
cd sparquet-studio
npm run dev                                            # Studio em http://localhost:5273
node scripts/promo-shots.mjs --out ../docs/promo      # capturas
node scripts/promo-pdf.mjs --in ../docs/promo/promo.html --out ../docs/promo/sparquet-studio.pdf
```

O script de capturas usa Chrome local via `puppeteer-core`; se ele não estiver no
caminho padrão, aponte com `CHROME_PATH`. A sessão é única de propósito: os dados
de exemplo vivem no IndexedDB, então recarregar a página no meio perde o estado.

## DataFusion Comet

`datafusion-comet.pdf` — 14 páginas no mesmo formato, sobre o plugin nativo e o
ganho medido. Fonte: `comet.html`, que reaproveita o `<style>` de `promo.html`;
as capturas ficam em `comet/` (também fora do versionamento).

O deck compara duas execuções do **mesmo Job**, e a comparação só é honesta com
dois runners separados: `spark.plugins` só vale na criação da SparkSession, e o
runner mantém uma sessão por processo. Um runner sobe com o jar do Comet no
`--driver-class-path`, o outro sem nenhum jar por perto.

```bash
# 1. os dados — 40M linhas, geradas uma vez, em processo próprio e sem Comet
python -c "from tests.io.integration.bench_comet import _prepara; \
           from pathlib import Path; _prepara(Path('/tmp/sparquet-bench-comet'), 40_000_000)"

# 2. os dois runners, cada um em seu terminal (Linux: a lib nativa do Comet é só para Linux)
PYSPARK_SUBMIT_ARGS="--jars $JAR --driver-class-path $JAR pyspark-shell" \
  python -m uvicorn server.main:app --port 8788      # com Comet
python -m uvicorn server.main:app --port 8789        # sem Comet

# 3. os dois Jobs, no workspace que os runners compartilham
cd sparquet-studio
npx vite-node scripts/comet-seed.ts -- --url http://localhost:8788 --token dev-local-token

# 4. capturas e PDF
npm run dev                                          # Studio em http://localhost:5273
node scripts/comet-shots.mjs --out ../docs/promo/comet --token dev-local-token
node scripts/promo-pdf.mjs --in ../docs/promo/comet.html --out ../docs/promo/datafusion-comet.pdf
```

O script de capturas roda cada Job de verdade — a primeira execução de cada
runner paga JIT e page cache frio, então descarte-a antes de fotografar o
histórico. Os números impressos no deck vêm de `tests/io/integration/bench_comet.py`,
que mede em processo separado, fora do runner.
