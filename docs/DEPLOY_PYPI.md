# Deploy no PyPI — publicar o `sparquet` como biblioteca

Guia para empacotar e publicar o projeto no PyPI, de forma que possa ser instalado
com `pip install sparquet` e usado como biblioteca.

O empacotamento já está configurado em [`pyproject.toml`](../pyproject.toml):
- nome de distribuição: **`sparquet`** (import: `sparquet`);
- **versão é fonte única** em `sparquet/__init__.py` (`__version__`), lida
  dinamicamente pelo setuptools (`[tool.setuptools.dynamic]`);
- `tests*`, `examples*`, `docs*` são excluídos do pacote;
- dependência base: `pyspark>=3.4.0`; extra opcional: `sparquet[delta]`.

---

## 1. Pré-requisitos (uma vez)

1. **Contas**: criar conta no [PyPI](https://pypi.org/account/register/) e no
   [TestPyPI](https://test.pypi.org/account/register/) (ambiente de teste).
2. **2FA + API tokens**: habilite 2FA e gere um **API token** em cada um
   (Account settings → API tokens). Use o token como senha (usuário `__token__`).
3. **Ferramentas de build/publish** (num venv):
   ```bash
   python -m pip install --upgrade build twine
   ```

---

## 2. Subir a versão

A versão fica **só** em `sparquet/__init__.py`:
```python
__version__ = "0.2.1"   # bump aqui (segue SemVer: MAJOR.MINOR.PATCH)
```
Não edite a versão no `pyproject.toml` — ele lê desse atributo. Faça commit do bump.

> Regra prática: PATCH = correção; MINOR = nova feature compatível; MAJOR = quebra.

---

## 3. Build do pacote

Na raiz do projeto:
```bash
rm -rf dist build *.egg-info        # limpa artefatos antigos
python -m build                     # gera dist/*.whl e dist/*.tar.gz
```
Isso produz, em `dist/`:
- `sparquet-<versão>-py3-none-any.whl` (wheel)
- `sparquet-<versão>.tar.gz` (sdist)

Valide os metadados antes de publicar:
```bash
python -m twine check dist/*
```

> Nota: o setuptools lê `__version__` por análise estática do `__init__.py`, sem
> importar o pacote — então o build **não exige** PySpark instalado no ambiente.

---

## 4. Publicar no TestPyPI (ensaio) e validar

Sempre teste no TestPyPI antes do PyPI real:
```bash
python -m twine upload --repository testpypi dist/*
```
Instale a partir do TestPyPI num venv limpo (puxando deps reais do PyPI):
```bash
python -m venv /tmp/venv && source /tmp/venv/bin/activate   # Windows: .\venv\Scripts\activate
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ \
            sparquet
python -c "import sparquet; print(sparquet.__version__)"
```

---

## 5. Publicar no PyPI (produção)

```bash
python -m twine upload dist/*
```
Pronto — o pacote fica disponível para:
```bash
pip install sparquet            # núcleo (pyspark)
pip install "sparquet[delta]"   # com Delta Lake OSS (fora do Databricks)
```

### Credenciais sem digitar toda vez (`~/.pypirc`)
```ini
[distutils]
index-servers = pypi testpypi

[pypi]
  username = __token__
  password = pypi-<seu-token>

[testpypi]
  repository = https://test.pypi.org/legacy/
  username = __token__
  password = pypi-<seu-token-testpypi>
```

---

## 6. Usando como biblioteca (consumidor)

```python
from sparquet import Sparquet

fw = Sparquet(spark={"app_name": "MeuJob"})
resultado = fw.run("meu_pipeline.json", params={"tipo_ativo": "NC"})
print(resultado.summary())
fw.stop()
```
Também há o entrypoint de CLI `sparquet` (definido em `[project.scripts]`).

---

## 7. Checklist de release

- [ ] `__version__` atualizado e commitado (SemVer).
- [ ] `CHANGELOG`/notas de release atualizados (se houver).
- [ ] `rm -rf dist build *.egg-info && python -m build`.
- [ ] `twine check dist/*` sem erros.
- [ ] Publicado e testado no **TestPyPI**.
- [ ] `twine upload dist/*` no PyPI.
- [ ] Tag git da versão: `git tag v<versão> && git push --tags`.

---

## 8. Deploy automatizado (CI/CD)

O fluxo das seções 3–5 já está automatizado em GitHub Actions — na prática, publicar
uma release faz tudo (testes → build → publish). Os passos manuais acima continuam
válidos como fallback ou para publicar de uma máquina local.

### Workflows

| Arquivo | Dispara em | O que faz |
|---|---|---|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | push / PR na `main` | roda os testes puros numa matriz Python (3.9 / 3.11 / 3.12) |
| [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) | **Release publicado** | testes → build (`+ twine check`) → **publish no PyPI** |
| idem | **execução manual** (Actions → *Run workflow*) | mesma esteira, mas **publish no TestPyPI** (ensaio) |

Em `publish.yml` o `build`/`publish` só rodam se o job de `test` passar — o teste é o
portão do release.

### Trusted Publishing (OIDC) — sem token manual

A publicação usa o [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) do
PyPI: o GitHub Actions troca um token OIDC de curta duração, então **não há token/segredo
de API** guardado no repo (por isso `permissions: id-token: write` nos jobs de publish).

Configuração (uma vez, em cada índice):

1. Em **pypi.org** → projeto `sparquet` → *Manage → Publishing* → **Add a trusted publisher**
   (GitHub). Se o projeto ainda não existe, use *pending publisher* (Account → Publishing).
2. Preencha: owner `VictorPasqualini`, repositório `sparquet`, workflow `publish.yml`,
   environment `pypi`.
3. Repita em **test.pypi.org** com environment `testpypi`.

> Alternativa por token: se preferir não usar OIDC, remova o bloco `permissions` e passe
> `password: ${{ secrets.PYPI_API_TOKEN }}` ao `pypa/gh-action-pypi-publish`.

### Publicar uma versão (fluxo recomendado)

```bash
# 1. bump da versão + commit
#    edite __version__ em sparquet/__init__.py
git add sparquet/__init__.py && git commit -m "release: v0.2.2"

# 2. (opcional) ensaio no TestPyPI: Actions → "Publish to PyPI" → Run workflow

# 3. tag + release no GitHub → dispara o publish no PyPI real
git tag v0.2.2 && git push --tags
gh release create v0.2.2 --generate-notes
```

> A versão do pacote vem do `__version__` (não da tag). Mantenha a tag e o `__version__`
> em sincronia para evitar confusão.
