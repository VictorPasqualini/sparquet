"""Where a Studio library lives: real JSON files on disk, not browser storage.

Until this module existed, everything a person built in Studio — workflows, jobs,
pipelines — lived only in the browser's IndexedDB. That made the library invisible
to git, unreadable by the framework CLI, and one "clear site data" away from gone.

The workspace fixes all three by writing two things per record:

* a **readable file**, the artefact a human reviews in a pull request. For a Job
  that is the compiled Sparquet pipeline JSON — the very thing
  `python -m sparquet run <file>` executes — so the diff a reviewer reads is the
  diff the cluster runs.
* a **sidecar** under `.studio/`, the full Studio record: canvas positions, notes,
  labels, params. Everything the editor needs to reopen the record exactly as it
  was, and nothing a reviewer wants to read.

Layout::

    <root>/
      .studio/
        index.json                      id -> readable path, so a rename moves a file
        workflow/<id>.json              the Studio records, authoritative on read
        job/<id>.json
        pipeline/<id>.json
      <workflow-slug>/
        workflow.json
        jobs/<job-slug>.json            runnable Sparquet config
        pipelines/<pipeline-slug>.json  stage order, by job id

Storage is kept behind `WorkspaceStore` for the same reason `ExecutionRepository`
is: an `S3WorkspaceStore` or a `GitWorkspaceStore` replaces `FileWorkspaceStore`
without the runner or the Studio changing at all. That is the whole point of the
seam — this runs on a laptop today and is meant to move to a shared host later.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

# The three record kinds a Studio library is made of. Plural forms are only ever
# used in URLs and directory names; the singular is the kind itself.
WORKFLOW = "workflow"
JOB = "job"
PIPELINE = "pipeline"
KINDS = (WORKFLOW, JOB, PIPELINE)

_STUDIO_DIR = ".studio"
_INDEX_FILE = "index.json"
_META_FILE = "meta.json"

# ------------------------------------------------- where the library lives

#: Where the chosen location is remembered. Deliberately not inside the
#: workspace: a setting that says where the workspace is cannot live in it.
_SETTINGS_FILE = "studio.json"
_ROOT_KEY = "workspace"
_DEFAULT_DIR = "workspace"


def data_home() -> Path:
    """The per-user directory this machine keeps application data in.

    The runner must never write into its own source tree. A checkout is code: it
    gets pulled, reset, moved and deleted, and a library living inside one is a
    library that disappears with a `git clean` — or, worse, one that turns up as
    noise in every `git status` and eventually gets committed by accident. So the
    default is the platform's own place for user data, and `SPARQUET_HOME`
    overrides the lot for anyone who wants everything in one directory.
    """
    override = os.getenv("SPARQUET_HOME")
    if override:
        return Path(override).expanduser()
    if os.name == "nt":
        base = os.getenv("APPDATA") or "~/AppData/Roaming"
        return Path(base).expanduser() / "Sparquet"
    base = os.getenv("XDG_DATA_HOME") or "~/.local/share"
    return Path(base).expanduser() / "sparquet"


def settings_path() -> Path:
    """The file `remember_root` writes."""
    return data_home() / _SETTINGS_FILE


def default_root() -> Path:
    """Where a library goes when nobody has said otherwise."""
    return data_home() / _DEFAULT_DIR


@dataclass(frozen=True)
class Location:
    """A resolved workspace root, and why it is that one.

    `source` is what the interface shows: somebody who cannot find their Jobs is
    almost always looking at a different root than the runner is, and naming the
    reason ("the SPARQUET_STUDIO_WORKSPACE variable") answers that in one line.
    """

    root: Path
    #: One of `env`, `settings`, `legacy`, `default`.
    source: str


def read_setting(key: str) -> Optional[Any]:
    """One value from the settings file, or None when there is nothing to read.

    A settings file that is corrupt is treated as absent. It holds preferences,
    not data: refusing to start because a preference will not parse would trade a
    small problem for a total one.
    """
    payload = _read_json(settings_path())
    if not isinstance(payload, dict):
        return None
    return payload.get(key)


def write_setting(key: str, value: Any) -> None:
    """Changes one value in the settings file, leaving the rest alone."""
    payload = _read_json(settings_path())
    settings = dict(payload) if isinstance(payload, dict) else {}
    if value is None:
        settings.pop(key, None)
    else:
        settings[key] = value
    _write_json(settings_path(), settings)


def remember_root(root: Path) -> Path:
    """Records where the library should live, from now on and after a restart."""
    resolved = Path(root).expanduser().resolve()
    write_setting(_ROOT_KEY, str(resolved))
    return resolved


def resolve_root(legacy: Optional[Path] = None) -> Location:
    """Where this runner's library lives.

    In order: the `SPARQUET_STUDIO_WORKSPACE` variable, which a deployment sets
    and nothing may override; then what somebody chose in the interface; then a
    `legacy` directory that already holds a library, adopted rather than
    abandoned, because a default that moved is no reason to lose what is in the
    old place; then the per-user default.
    """
    from_env = os.getenv("SPARQUET_STUDIO_WORKSPACE")
    if from_env:
        return Location(Path(from_env).expanduser(), "env")

    chosen = read_setting(_ROOT_KEY)
    if isinstance(chosen, str) and chosen.strip():
        return Location(Path(chosen).expanduser(), "settings")

    if legacy is not None and (Path(legacy) / _STUDIO_DIR).is_dir():
        return Location(Path(legacy), "legacy")

    return Location(default_root(), "default")


# A slug is what makes the tree readable in a diff. Anything a file system or a
# reviewer would struggle with collapses to a dash.
_SLUG_STRIP = re.compile(r"[^a-z0-9]+")


class WorkspaceError(Exception):
    """A request the workspace refuses: unknown kind, id that escapes the root."""


def slugify(value: str, fallback: str = "untitled") -> str:
    """`Orders — daily (v2)` -> `orders-daily-v2`. Never empty, never a path.

    Accents are folded rather than dropped: most of this product's users name
    things in Portuguese, and `ingest-o.json` for `Ingestão` would make the tree
    unreadable exactly where readability is the point.
    """
    folded = unicodedata.normalize("NFKD", str(value or ""))
    ascii_only = "".join(char for char in folded if not unicodedata.combining(char))
    slug = _SLUG_STRIP.sub("-", ascii_only.strip().lower()).strip("-")
    return slug[:60] or fallback


@dataclass
class Document:
    """One record of a Studio library, in the two forms it is stored as."""

    kind: str
    id: str
    #: The Studio record itself — what the editor reads back.
    record: Dict[str, Any]
    #: A Job's compiled Sparquet JSON, when the client sent one. This is what the
    #: readable file holds, and what the framework can run unchanged.
    config: Optional[Dict[str, Any]] = None
    #: Relative path of the readable file, filled in by the store on write.
    path: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "id": self.id,
            "record": self.record,
            "path": self.path,
        }


@dataclass
class WorkspaceSnapshot:
    """Everything the workspace holds, in one read — how Studio loads its library."""

    root: str
    workflows: List[Document] = field(default_factory=list)
    jobs: List[Document] = field(default_factory=list)
    pipelines: List[Document] = field(default_factory=list)
    #: Small bookkeeping values that belong to the library rather than to a record:
    #: which storage version wrote it, whether the examples were already seeded.
    #: They travel with the workspace so a second machine does not re-migrate or
    #: re-seed a library that is already current.
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "workflows": [doc.to_json() for doc in self.workflows],
            "jobs": [doc.to_json() for doc in self.jobs],
            "pipelines": [doc.to_json() for doc in self.pipelines],
            "meta": self.meta,
        }


class WorkspaceStore(Protocol):
    def describe(self) -> Dict[str, Any]: ...

    def snapshot(self) -> WorkspaceSnapshot: ...

    def read(self, kind: str, doc_id: str) -> Optional[Document]: ...

    def write(self, doc: Document) -> Document: ...

    def read_meta(self) -> Dict[str, Any]: ...

    def write_meta(self, key: str, value: Any) -> None: ...

    def delete_meta(self, key: str) -> None: ...

    def delete(self, kind: str, doc_id: str) -> bool: ...


def _check_kind(kind: str) -> str:
    if kind not in KINDS:
        raise WorkspaceError(f"Unknown record kind: {kind!r}. Expected one of {KINDS}.")
    return kind


def _check_id(doc_id: str) -> str:
    """Ids come from the client, so they address a file — they are checked, not trusted."""
    value = str(doc_id or "").strip()
    if not value or "/" in value or "\\" in value or value.startswith("."):
        raise WorkspaceError(f"Unusable record id: {doc_id!r}.")
    return value


def _write_json(path: Path, payload: Any) -> None:
    """Writes JSON the way a repository wants it: two-space indent, one trailing
    newline, UTF-8 kept as UTF-8 — a diff of a renamed column should be one line,
    not the whole file. The write goes through a temporary file in the same
    directory so a crash mid-write never leaves a half-written config behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n"
    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, prefix=".tmp-", suffix=".json", delete=False
    )
    try:
        with handle as out:
            out.write(text)
        os.replace(handle.name, path)
    except BaseException:
        Path(handle.name).unlink(missing_ok=True)
        raise


def _read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # A file someone hand-edited into invalid JSON is reported as absent rather
        # than crashing the whole library load.
        return None


class FileWorkspaceStore:
    """`WorkspaceStore` backed by a directory of real files.

    One process-wide lock, like `SQLiteExecutionRepository`: the write volume is a
    handful of files per save, and a lock is far simpler than making the index file
    safe for concurrent readers and writers.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()
        self._lock = threading.RLock()
        self._root.mkdir(parents=True, exist_ok=True)
        (self._root / _STUDIO_DIR).mkdir(parents=True, exist_ok=True)

    # ---- paths -----------------------------------------------------------

    @property
    def root(self) -> Path:
        return self._root

    def describe(self) -> Dict[str, Any]:
        return {"kind": "file", "root": str(self._root), "writable": os.access(self._root, os.W_OK)}

    def _sidecar(self, kind: str, doc_id: str) -> Path:
        return self._root / _STUDIO_DIR / kind / f"{doc_id}.json"

    def _index_path(self) -> Path:
        return self._root / _STUDIO_DIR / _INDEX_FILE

    def _index(self) -> Dict[str, str]:
        """`"<kind>:<id>" -> relative readable path`. Rebuilt lazily; a missing or
        corrupt index costs a rewrite of the readable files, never a lost record —
        the sidecars are what a read returns."""
        value = _read_json(self._index_path())
        return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}

    def _save_index(self, index: Dict[str, str]) -> None:
        _write_json(self._index_path(), dict(sorted(index.items())))

    def _resolve(self, relative: str) -> Path:
        """A path out of the index still has to land inside the root."""
        candidate = (self._root / relative).resolve()
        if candidate != self._root and self._root not in candidate.parents:
            raise WorkspaceError(f"Path escapes the workspace: {relative!r}.")
        return candidate

    # ---- readable layout -------------------------------------------------

    def _workflow_dir(self, workflow_id: Optional[str], names: Dict[str, str]) -> str:
        """Directory a record's readable file belongs in. A record whose workflow is
        unknown lands in `_orphans/`, visible rather than silently dropped."""
        if not workflow_id:
            return "_orphans"
        return slugify(names.get(workflow_id, workflow_id), fallback=slugify(workflow_id))

    def _readable_path(self, doc: Document, names: Dict[str, str]) -> str:
        record = doc.record
        name = str(record.get("name") or doc.id)
        if doc.kind == WORKFLOW:
            return f"{slugify(name, fallback=slugify(doc.id))}/workflow.json"
        folder = self._workflow_dir(record.get("workflowId"), names)
        sub = "jobs" if doc.kind == JOB else "pipelines"
        return f"{folder}/{sub}/{slugify(name, fallback=slugify(doc.id))}.json"

    def _workflow_names(self) -> Dict[str, str]:
        names: Dict[str, str] = {}
        for doc in self._read_kind(WORKFLOW):
            name = doc.record.get("name")
            if isinstance(name, str) and name:
                names[doc.id] = name
        return names

    def _readable_payload(self, doc: Document) -> Any:
        """What the reviewable file holds — the artefact, never the canvas.

        A Job's is its compiled Sparquet JSON, so the file in the repository is the
        file the framework runs. A Pipeline's is its stage order by job id. A
        Workflow's is its own small record: there is nothing else to it.
        """
        if doc.kind == JOB:
            if doc.config is not None:
                return doc.config
            # No compiled config sent (an older Studio, or a Job that does not
            # compile yet): the record keeps the file honest instead of empty.
            return {"_studio": "This job has no compiled configuration yet.", "id": doc.id}
        if doc.kind == PIPELINE:
            record = doc.record
            stages = record.get("stages") if isinstance(record.get("stages"), list) else []
            links = record.get("links") if isinstance(record.get("links"), list) else []
            return {
                "name": record.get("name"),
                "description": record.get("description"),
                "stages": [
                    {"id": stage.get("id"), "job_id": stage.get("jobId")}
                    for stage in stages
                    if isinstance(stage, dict)
                ],
                "links": [
                    {"source": link.get("source"), "target": link.get("target")}
                    for link in links
                    if isinstance(link, dict)
                ],
            }
        return doc.record

    # ---- reads -----------------------------------------------------------

    def _read_kind(self, kind: str) -> List[Document]:
        folder = self._root / _STUDIO_DIR / kind
        if not folder.is_dir():
            return []
        docs: List[Document] = []
        for path in sorted(folder.glob("*.json")):
            record = _read_json(path)
            if isinstance(record, dict):
                docs.append(Document(kind=kind, id=path.stem, record=record))
        return docs

    def snapshot(self) -> WorkspaceSnapshot:
        with self._lock:
            index = self._index()
            by_kind = {kind: self._read_kind(kind) for kind in KINDS}
            for kind, docs in by_kind.items():
                for doc in docs:
                    doc.path = index.get(f"{kind}:{doc.id}")
            return WorkspaceSnapshot(
                root=str(self._root),
                workflows=by_kind[WORKFLOW],
                jobs=by_kind[JOB],
                pipelines=by_kind[PIPELINE],
                meta=self.read_meta(),
            )

    def read(self, kind: str, doc_id: str) -> Optional[Document]:
        kind = _check_kind(kind)
        doc_id = _check_id(doc_id)
        with self._lock:
            record = _read_json(self._sidecar(kind, doc_id))
            if not isinstance(record, dict):
                return None
            return Document(
                kind=kind, id=doc_id, record=record, path=self._index().get(f"{kind}:{doc_id}")
            )

    # ---- meta ------------------------------------------------------------

    def _meta_path(self) -> Path:
        return self._root / _STUDIO_DIR / _META_FILE

    def read_meta(self) -> Dict[str, Any]:
        value = _read_json(self._meta_path())
        return dict(value) if isinstance(value, dict) else {}

    def write_meta(self, key: str, value: Any) -> None:
        with self._lock:
            meta = self.read_meta()
            meta[str(key)] = value
            _write_json(self._meta_path(), dict(sorted(meta.items())))

    def delete_meta(self, key: str) -> None:
        with self._lock:
            meta = self.read_meta()
            if str(key) in meta:
                del meta[str(key)]
                _write_json(self._meta_path(), dict(sorted(meta.items())))

    # ---- writes ----------------------------------------------------------

    def write(self, doc: Document) -> Document:
        kind = _check_kind(doc.kind)
        doc_id = _check_id(doc.id)
        with self._lock:
            names = self._workflow_names()
            if kind == WORKFLOW:
                name = doc.record.get("name")
                if isinstance(name, str) and name:
                    names[doc_id] = name
            relative = self._readable_path(Document(kind, doc_id, doc.record, doc.config), names)
            index = self._index()
            key = f"{kind}:{doc_id}"
            previous = index.get(key)

            _write_json(self._sidecar(kind, doc_id), doc.record)
            target = self._resolve(relative)
            _write_json(target, self._readable_payload(doc))

            # A rename leaves the old file behind unless it is removed here, and a
            # stale `orders.json` next to `orders-daily.json` is exactly the kind of
            # thing that gets run by mistake.
            if previous and previous != relative:
                self._remove(previous)
            index[key] = relative
            self._save_index(index)

            # Renaming a Workflow moves everything under it, so the tree keeps
            # matching the names on screen instead of drifting one release at a time.
            if kind == WORKFLOW:
                self._relocate_children(doc_id, names, index)

            return Document(kind=kind, id=doc_id, record=doc.record, config=doc.config,
                            path=relative)

    def _relocate_children(
        self, workflow_id: str, names: Dict[str, str], index: Dict[str, str]
    ) -> None:
        moved = False
        for kind in (JOB, PIPELINE):
            for doc in self._read_kind(kind):
                if doc.record.get("workflowId") != workflow_id:
                    continue
                key = f"{kind}:{doc.id}"
                relative = self._readable_path(doc, names)
                current = index.get(key)
                if current == relative:
                    continue
                source = self._resolve(current) if current else None
                target = self._resolve(relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                if source is not None and source.is_file():
                    os.replace(source, target)
                index[key] = relative
                moved = True
        if moved:
            self._save_index(index)
            self._prune_empty_dirs()

    def delete(self, kind: str, doc_id: str) -> bool:
        kind = _check_kind(kind)
        doc_id = _check_id(doc_id)
        with self._lock:
            sidecar = self._sidecar(kind, doc_id)
            existed = sidecar.is_file()
            sidecar.unlink(missing_ok=True)
            index = self._index()
            relative = index.pop(f"{kind}:{doc_id}", None)
            if relative:
                self._remove(relative)
            self._save_index(index)
            self._prune_empty_dirs()
            return existed

    def _remove(self, relative: str) -> None:
        try:
            self._resolve(relative).unlink(missing_ok=True)
        except WorkspaceError:
            pass

    def _prune_empty_dirs(self) -> None:
        """Deleting the last job of a workflow should not leave an empty `jobs/`
        directory that git cannot even record."""
        for path in sorted(self._root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if path.is_dir() and _STUDIO_DIR not in path.parts and not any(path.iterdir()):
                try:
                    path.rmdir()
                except OSError:
                    pass

    # ---- maintenance -----------------------------------------------------

    def clear(self) -> None:
        """Empties the workspace. Only ever called by a test or an explicit reset."""
        with self._lock:
            for child in self._root.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
            (self._root / _STUDIO_DIR).mkdir(parents=True, exist_ok=True)
