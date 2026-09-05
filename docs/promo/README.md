# Deck promocional

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
