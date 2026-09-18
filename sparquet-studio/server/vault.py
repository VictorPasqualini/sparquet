"""Connection secrets — values the runner resolves and nobody reads back.

A JDBC source needs a password, and a pipeline JSON is a document people copy
into chat, commit to git and paste into a ticket. So the JSON holds a reference,
`{secret:pg-prod/password}`, and the value it stands for lives here.

Three properties shape everything below.

**The framework never learns that secrets exist.** The reference is replaced in
the copy of the document handed to Spark, and the framework receives an ordinary
string. `{secret:...}` is invisible to `sparquet.utils.template`, whose `{name}`
pattern is `\\w+` and therefore cannot match a reference containing `:` and `/`,
and equally invisible to the `{{runtime}}` variables of the transformation
engine. Three syntaxes, no overlap, and only this one is resolved off-document.

**A value leaves this module only towards Spark.** There is no endpoint that
returns one, for anybody, at any access level. What a screen may know is that a
secret named `pg-prod` exists, which fields it carries and who owns it.

**Where the value lives is a choice, not a fact.** A `local` secret is encrypted
into the workspace with a master key the runner holds; an `env` secret is a name
this process reads from its own environment, which is how a container, a CI job
or any cloud runtime already hands credentials to a process. Both are the same
record with the same rules — only the resolver differs, and a new provider is a
function in `_RESOLVERS`, not a new concept.

The module is named `vault` rather than `secrets` because `secrets` is a stdlib
module the runner already imports for token generation, and shadowing it in a
flat module directory would replace it everywhere.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

#: The master key a `local` secret is encrypted with. Deliberately an environment
#: variable and not a file in the workspace: a workspace is copied, synced and
#: backed up, and a key sitting next to the ciphertext protects nothing.
MASTER_KEY_ENV = "SPARQUET_STUDIO_SECRET_KEY"

#: Where a value may come from. `local` is the runner's own encrypted store;
#: `env` is a variable in the runner's environment, which is what every cloud
#: secret manager ends up as by the time a process can read it.
PROVIDERS = ("local", "env")

#: `{secret:pg-prod/password}` — the name of a secret and one field inside it.
#: A secret is a bundle rather than a single value because a connection is a
#: bundle: url, user and password are rotated together and belong together.
REFERENCE = re.compile(
    r"\{secret:([A-Za-z0-9][A-Za-z0-9._-]*)/([A-Za-z0-9][A-Za-z0-9._-]*)\}"
)

#: What a name may be once normalized. Same shape as a tag, and for the same
#: reason: it is typed by hand in one place and matched in another.
_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

#: Below this length, masking a value would blank out unrelated text — a value
#: of "1" appears in every timestamp. Short credentials are not protected in
#: error strings, which is one more reason not to have any.
_MASKABLE = 4


class SecretError(Exception):
    """Anything a caller did wrong, or a key the runner does not have.

    Carries a message written for the person who will read it in the Studio, not
    a stack trace: every one of these is either a missing configuration or a
    reference to something that is not there.
    """


def normalize(name: str) -> str:
    """A name as it is stored and matched — trimmed and lower-cased.

    Same normalization as a tag, applied on both sides, so `PG-Prod` typed in the
    form reaches the rule written on `pg-prod`.
    """
    return (name or "").strip().lower()


def check_name(name: str) -> str:
    """The normalized name, or a refusal explaining what a name may be."""
    clean = normalize(name)
    if not _NAME.match(clean):
        raise SecretError(
            f"{name!r} is not a usable secret name. Use letters, digits, '.', '-' "
            "or '_', starting with a letter or a digit — the name goes into a "
            "reference like {secret:pg-prod/password}, which has to survive being "
            "read by a person."
        )
    return clean


@dataclass
class Secret:
    """One bundle of connection material, under one name.

    `fields` maps a field name to its stored material, and what that material is
    depends on the provider: the sealed token for `local`, the name of an
    environment variable for `env`. The distinction never leaves this module —
    every caller asks for a value and gets a value.
    """

    name: str
    provider: str = "local"
    description: str = ""
    #: The catalog's own vocabulary, so a deny on `tag/pii` closes the credential
    #: as well as the table. A secret without tags is governed by its name alone.
    tags: List[str] = field(default_factory=list)
    fields: Dict[str, str] = field(default_factory=dict)
    updated_at: float = 0.0
    updated_by: str = ""

    def as_dict(self) -> Dict[str, Any]:
        """The stored shape: camelCase, like the rest of the meta record."""
        return {
            "name": self.name,
            "provider": self.provider,
            "description": self.description,
            "tags": list(self.tags),
            "fields": dict(self.fields),
            "updatedAt": self.updated_at,
            "updatedBy": self.updated_by,
        }

    def redacted(self) -> Dict[str, Any]:
        """What a screen is allowed to know.

        The field names are here because a form has to offer them; the material
        behind them is not, in any provider. An `env` secret also reports which
        variables it reads, which is an operational fact rather than a
        credential — knowing that `PGPASSWORD` is consulted tells nobody what it
        holds, and not knowing it makes a misconfigured runner impossible to
        diagnose from the screen.
        """
        out: Dict[str, Any] = {
            "name": self.name,
            "provider": self.provider,
            "description": self.description,
            "tags": list(self.tags),
            "fields": sorted(self.fields),
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }
        if self.provider == "env":
            out["binding"] = dict(self.fields)
        return out


@dataclass
class Store:
    """Every secret this runner holds, plus the salt its sealing key derives from.

    The salt is per-store and generated once: two runners sharing a master key
    still derive different encryption keys, so a ciphertext copied between
    workspaces is inert.
    """

    salt: str = ""
    items: Dict[str, Secret] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "salt": self.salt,
            "items": {name: item.as_dict() for name, item in sorted(self.items.items())},
        }


def load(raw: Any) -> Store:
    """The stored record, defensively. Anything unreadable is no secrets at all.

    A corrupt store must not take the runner down on boot: every caller here
    treats "no secret by that name" as an error with a message, and that is a far
    better failure than a runner that will not start.
    """
    if not isinstance(raw, dict):
        return Store()
    items: Dict[str, Secret] = {}
    stored = raw.get("items")
    if not isinstance(stored, dict):
        return Store(salt=str(raw.get("salt") or ""), items=items)
    for key, value in stored.items():
        if not isinstance(value, dict):
            continue
        name = normalize(str(value.get("name") or key))
        if not _NAME.match(name):
            continue
        provider = str(value.get("provider") or "local")
        if provider not in PROVIDERS:
            continue
        fields = value.get("fields")
        tags = value.get("tags")
        items[name] = Secret(
            name=name,
            provider=provider,
            description=str(value.get("description") or ""),
            tags=[normalize(tag) for tag in tags if str(tag).strip()]
            if isinstance(tags, list)
            else [],
            fields={str(k): str(v) for k, v in fields.items()}
            if isinstance(fields, dict)
            else {},
            updated_at=float(value.get("updatedAt") or 0.0),
            updated_by=str(value.get("updatedBy") or ""),
        )
    return Store(salt=str(raw.get("salt") or ""), items=items)


# --------------------------------------------------------------- encryption


def _fernet_module() -> Tuple[Any, Any]:
    """`cryptography`, or a refusal that says what to do instead.

    Imported here and not at module load so that a runner without the package
    still starts, still resolves `env` secrets and still serves every other
    endpoint. Only the local store needs it.
    """
    try:
        from cryptography.fernet import Fernet, InvalidToken
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise SecretError(
            "Encrypting a secret needs the `cryptography` package: install it with "
            "`pip install -r sparquet-studio/server/requirements.txt`. Until then "
            "use the `env` provider, which keeps the value out of the workspace "
            "entirely."
        ) from error
    return Fernet, InvalidToken


def master_key() -> str:
    """The key `local` secrets are sealed with, or a refusal.

    There is deliberately no fallback. A runner with no key could only store the
    value in the clear, and a store that silently degrades to plaintext is worse
    than one that will not accept the secret at all — the first looks like it
    worked.
    """
    key = os.getenv(MASTER_KEY_ENV, "").strip()
    if not key:
        raise SecretError(
            f"{MASTER_KEY_ENV} is not set, so this runner cannot encrypt a secret "
            "and will not store one in the clear. Set it to a long random string "
            "(and keep it — the stored secrets cannot be read without it), or use "
            "the `env` provider instead."
        )
    if len(key) < 16:
        raise SecretError(
            f"{MASTER_KEY_ENV} is shorter than 16 characters. It is the only thing "
            "standing between a copied workspace file and every credential in it."
        )
    return key


def new_salt() -> str:
    return base64.urlsafe_b64encode(os.urandom(16)).decode("ascii")


def _cipher(salt: str) -> Any:
    fernet, _ = _fernet_module()
    derived = hashlib.scrypt(
        master_key().encode("utf-8"),
        salt=base64.urlsafe_b64decode(salt.encode("ascii")),
        n=2**14,
        r=8,
        p=1,
        dklen=32,
    )
    return fernet(base64.urlsafe_b64encode(derived))


def seal(value: str, salt: str) -> str:
    """One value, encrypted for storage."""
    return _cipher(salt).encrypt(value.encode("utf-8")).decode("ascii")


def unseal(token: str, salt: str) -> str:
    _, invalid_token = _fernet_module()
    try:
        return _cipher(salt).decrypt(token.encode("ascii")).decode("utf-8")
    except invalid_token as error:
        raise SecretError(
            f"This secret cannot be decrypted with the key this runner holds. "
            f"Either {MASTER_KEY_ENV} changed, or the workspace was written by "
            "another runner — rotate the secret to store it again under the "
            "current key."
        ) from error


# --------------------------------------------------------------- resolution


def _from_local(secret: Secret, field_name: str, salt: str) -> str:
    return unseal(secret.fields[field_name], salt)


def _from_env(secret: Secret, field_name: str, salt: str) -> str:
    variable = secret.fields[field_name]
    value = os.getenv(variable)
    if value is None:
        raise SecretError(
            f"{secret.name}/{field_name} reads the environment variable "
            f"{variable}, which is not set on this runner. Whatever supplies it — "
            "the container, the CI job, the cloud secret manager — is not reaching "
            "this process."
        )
    return value


#: A provider is a function from (secret, field, salt) to a value. Adding a
#: cloud manager means adding one entry here and one name to `PROVIDERS`;
#: nothing else in the runner, the IAM model or the Studio changes.
_RESOLVERS: Dict[str, Callable[[Secret, str, str], str]] = {
    "local": _from_local,
    "env": _from_env,
}


def value_of(store: Store, name: str, field_name: str) -> str:
    """The value behind one reference. The only way out of this module."""
    secret = store.items.get(normalize(name))
    if secret is None:
        raise SecretError(
            f"No secret named {normalize(name)!r} on this runner. It may have been "
            "deleted, or it may live on the runner this JSON was written against."
        )
    if field_name not in secret.fields:
        known = ", ".join(sorted(secret.fields)) or "none"
        raise SecretError(
            f"The secret {secret.name!r} has no field {field_name!r}. It carries: {known}."
        )
    resolver = _RESOLVERS.get(secret.provider)
    if resolver is None:  # pragma: no cover - `load` already rejects the record
        raise SecretError(f"Unknown secret provider {secret.provider!r}.")
    return resolver(secret, field_name, store.salt)


def references(document: Any) -> List[Tuple[str, str]]:
    """Every `{secret:name/field}` in a document, in order, without duplicates."""
    text = document if isinstance(document, str) else json.dumps(document)
    seen: List[Tuple[str, str]] = []
    for match in REFERENCE.finditer(text):
        pair = (normalize(match.group(1)), match.group(2))
        if pair not in seen:
            seen.append(pair)
    return seen


def names_in(document: Any) -> List[str]:
    """The distinct secrets a document names — what access has to be checked for."""
    out: List[str] = []
    for name, _ in references(document):
        if name not in out:
            out.append(name)
    return out


def render(document: Dict[str, Any], lookup: Callable[[str, str], str]) -> Tuple[Dict[str, Any], List[str]]:
    """A copy of the document with every reference replaced by its value.

    Substitution happens in the serialized JSON rather than by walking the tree,
    because a reference may sit anywhere — an option value, a URL fragment, half
    of a connection string — and `{secret:...}` is unambiguous enough to replace
    textually. The replacement is JSON-escaped: a password containing a quote or
    a backslash would otherwise produce a document that no longer parses, and
    the failure would arrive as a syntax error with the password in it.

    The input is never mutated. What is stored in history and shown on screen has
    to stay the document with the references still in it.
    """
    used: List[str] = []

    def replace(match: "re.Match[str]") -> str:
        value = lookup(normalize(match.group(1)), match.group(2))
        used.append(value)
        return json.dumps(value)[1:-1]

    rendered = REFERENCE.sub(replace, json.dumps(document))
    parsed = json.loads(rendered)
    if not isinstance(parsed, dict):  # pragma: no cover - input is always an object
        raise SecretError("Secret substitution produced a non-object document.")
    return parsed, used


def mask(text: str, values: Iterable[str]) -> str:
    """Every resolved value, blanked out of a string on its way to a person.

    This is not decoration. A JDBC driver that cannot connect raises an exception
    quoting the whole URL, password included, and that string travels into the
    run log, the SSE stream, the history and the screen. Masking happens at the
    boundary because there is no way to stop the driver from doing it.
    """
    if not text:
        return text
    out = text
    # Longest first: a value that contains another must not be half-replaced.
    for value in sorted({v for v in values if len(v) >= _MASKABLE}, key=len, reverse=True):
        out = out.replace(value, "***")
    return out


def scrub(value: Any, values: Iterable[str]) -> Any:
    """`mask`, applied to every string in a structure.

    Used on whole responses rather than on the one field an error is expected in:
    a credential that ends up somewhere unexpected is exactly the case worth
    covering, and that is the case a targeted mask misses.
    """
    secret_values = [v for v in values if len(v) >= _MASKABLE]
    if not secret_values:
        return value
    if isinstance(value, str):
        return mask(value, secret_values)
    if isinstance(value, list):
        return [scrub(item, secret_values) for item in value]
    if isinstance(value, dict):
        return {key: scrub(item, secret_values) for key, item in value.items()}
    return value


# ------------------------------------------------------------------ writing


def put(
    store: Store,
    name: str,
    *,
    provider: str,
    values: Dict[str, Optional[str]],
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    actor: str = "",
) -> Store:
    """Create or change one secret, returning the new store.

    `values` is a patch over the fields: a name mapped to a value sets it, a name
    mapped to `None` removes it, and a field left out is kept as it was. That is
    what rotation is — one field replaced without having to re-send the others,
    which the caller could not do anyway, having never been allowed to read them.
    """
    clean = check_name(name)
    if provider not in PROVIDERS:
        known = ", ".join(PROVIDERS)
        raise SecretError(f"Unknown provider {provider!r}. This runner has: {known}.")

    current = store.items.get(clean)
    salt = store.salt
    if provider == "local" and not salt:
        salt = new_salt()

    fields = dict(current.fields) if current and current.provider == provider else {}
    for key, raw in values.items():
        if not _NAME.match(normalize(key)):
            raise SecretError(
                f"{key!r} is not a usable field name. Use letters, digits, '.', '-' "
                "or '_' — the field name appears in the reference."
            )
        if raw is None:
            fields.pop(key, None)
            continue
        text = str(raw)
        if not text:
            raise SecretError(
                f"{clean}/{key} was given an empty value. Remove the field instead — "
                "an empty credential fails at connection time with a message that "
                "points at the database rather than at this."
            )
        fields[key] = seal(text, salt) if provider == "local" else text

    if not fields:
        raise SecretError(
            f"{clean} would hold no fields. A secret with nothing in it cannot be "
            "referenced; delete it instead."
        )

    updated = Secret(
        name=clean,
        provider=provider,
        description=(current.description if description is None and current else (description or "")),
        tags=[normalize(tag) for tag in (tags if tags is not None else (current.tags if current else [])) if str(tag).strip()],
        fields=fields,
        updated_at=time.time(),
        updated_by=actor,
    )
    items = dict(store.items)
    items[clean] = updated
    return Store(salt=salt, items=items)


def remove(store: Store, name: str) -> Store:
    clean = normalize(name)
    if clean not in store.items:
        raise SecretError(f"No secret named {clean!r} on this runner.")
    items = dict(store.items)
    items.pop(clean)
    return Store(salt=store.salt, items=items)


def check(store: Store, name: str) -> Dict[str, str]:
    """Whether every field of one secret resolves right now, field by field.

    Deliberately not a connection test: opening the database needs its driver on
    the classpath and a network route, and failing that tells you nothing about
    the secret. What this answers is the question the store is responsible for —
    is the material still there and still readable — and it answers it without
    the value reaching the caller.
    """
    secret = store.items.get(normalize(name))
    if secret is None:
        raise SecretError(f"No secret named {normalize(name)!r} on this runner.")
    out: Dict[str, str] = {}
    for field_name in sorted(secret.fields):
        try:
            value_of(store, secret.name, field_name)
            out[field_name] = "ok"
        except SecretError as error:
            out[field_name] = str(error)
    return out
