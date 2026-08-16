"""Binary files (nativo do Spark — `binaryFile`). SOMENTE LEITURA.

Lê arquivos inteiros como binário. O DataFrame tem as colunas: path (string),
modificationTime (timestamp), length (long), content (binary). Útil para imagens,
PDFs, blobs — normalmente seguido de um `filter`/`select` e uma transformação.

Opções: pathGlobFilter (ex: "*.pdf"), recursiveFileLookup, modifiedBefore/After.

O Spark não escreve neste formato — não há BinaryWriter (use parquet/delta para
persistir a coluna `content`).
"""
from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io._formats import read_via
from sparquet.io.base import BaseReader


class BinaryReader(BaseReader):
    def read(self) -> DataFrame:
        return read_via(self.spark, self.config, "binaryFile")
