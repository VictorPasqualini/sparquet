# Deploy no PyPI — publicar o `spark-framework` como biblioteca

Guia para empacotar e publicar o projeto no PyPI, de forma que possa ser instalado
com `pip install spark-framework` e usado como biblioteca.

O empacotamento já está configurado em [`pyproject.toml`](../pyproject.toml):
- nome de distribuição: **`spark-framework`** (import: `spark_framework`);
- **versão é fonte única** em `spark_framework/__init__.py` (`__version__`), lida
  dinamicamente pelo setuptools (`[tool.setuptools.dynamic]`);
- `tests*`, `examples*`, `docs*` são excluídos do pacote;
- dependência base: `pyspark>=3.4.0`; extra opcional: `spark-framework[delta]`.

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

A versão fica **só** em `spark_framework/__init__.py`:
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
- `spark_framework-<versão>-py3-none-any.whl` (wheel)
- `spark_framework-<versão>.tar.gz` (sdist)

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
            spark-framework
python -c "import spark_framework; print(spark_framework.__version__)"
```

---

## 5. Publicar no PyPI (produção)

```bash
python -m twine upload dist/*
```
Pronto — o pacote fica disponível para:
```bash
pip install spark-framework            # núcleo (pyspark)
pip install "spark-framework[delta]"   # com Delta Lake OSS (fora do Databricks)
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
from spark_framework import SparkFramework

fw = SparkFramework(spark={"app_name": "MeuJob"})
resultado = fw.run("meu_pipeline.json", params={"tipo_ativo": "NC"})
print(resultado.summary())
fw.stop()
```
Também há o entrypoint de CLI `spark-framework` (definido em `[project.scripts]`).

---

## 7. Checklist de release

- [ ] `__version__` atualizado e commitado (SemVer).
- [ ] `CHANGELOG`/notas de release atualizados (se houver).
- [ ] `rm -rf dist build *.egg-info && python -m build`.
- [ ] `twine check dist/*` sem erros.
- [ ] Publicado e testado no **TestPyPI**.
- [ ] `twine upload dist/*` no PyPI.
- [ ] Tag git da versão: `git tag v<versão> && git push --tags`.

> Futuro (ver [ROADMAP.md](../ROADMAP.md) §6): automatizar build+publish via CI ao
> criar uma tag de versão.
