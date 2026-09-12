"""Access rules over the things the runner has no records for.

`auth.py` answers "may this person query at all". It cannot answer "may this
person query THIS table", because a dataset is not a record the runner owns — it
is an address a Job happens to mention, and the set of them only exists once
somebody walks the library. So Studio writes the rules and stores them beside
the catalog annotations, in `.studio/meta.json` under `grants`, and this module
evaluates them here, where the query actually runs. A rule the browser alone
honoured would protect nothing: `/query` is an HTTP endpoint, and curl is not
obliged to run the UI.

The shape is Databricks' managed tables, because that is the model a data team
already has in its head:

  1. **Every securable has an owner.** The owner holds every privilege on it and
     may grant and revoke there, without holding `iam:ManageGrants` over the
     runner. Ownership is transferable, and it is the only thing an explicit
     deny cannot take away — an object whose owner can be locked out of it is an
     object nobody can fix.
  2. **Privileges are inherited downwards.** A grant on `/lake/silver` reaches
     `/lake/silver/orders`, the way a grant on a schema reaches its tables; a
     grant on a Workflow reaches the Jobs and Pipelines inside it. Levels are
     cumulative across the whole chain — the answer is the best any ancestor
     gives — so access is widened by naming the container, not by repeating the
     rule on every child.
  3. **A securable with NO rule anywhere in its chain is open** to whoever
     already holds the matching action. Anything else would break every runner
     that has never opened the permissions screen, and "somebody added a rule to
     an unrelated table and mine went dark" is not a failure mode worth having.
  4. **An explicit deny beats every allow**, at any level of the chain. A deny
     that can be widened away by adding another allow is not a deny. Unity
     Catalog has no DENY at all; this one does, so it needs a rule that holds,
     and "the most restrictive wins" is the only one that cannot be gamed from
     below.

Levels are cumulative: admin implies write implies read.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

#: Every resource kind a grant can name. Datasets are the reason this exists;
#: Jobs and Pipelines are here because the same question gets asked about them
#: and answering it in two places would let the two answers drift. A Workflow is
#: not asked about directly — it is the container the other two inherit from,
#: which is exactly what a catalog is to a table.
RESOURCE_KINDS = ("dataset", "job", "pipeline", "workflow")

#: The kinds that live inside a Workflow, and therefore inherit from one.
CONTAINED_KINDS = ("job", "pipeline")

LEVELS = ("read", "write", "admin")

LEVEL_RANK: Dict[str, int] = {"read": 1, "write": 2, "admin": 3}

#: Both in the resource id and in the principal id.
ANY = "*"

#: The separators a dataset address nests with: a path (`/lake/silver/orders`)
#: and a qualified name (`main.silver.orders`). One address uses one of them, so
#: the first that appears decides — an address mixing both nests by its path,
#: which is what `s3://bucket/main.db/orders` means.
_SEPARATORS = ("/", ".")


@dataclass(frozen=True)
class Grant:
    resource: str
    resource_id: str
    principal_kind: str
    principal_id: str
    level: str
    effect: str


@dataclass(frozen=True)
class Owner:
    """Who a securable belongs to. One per resource — ownership is not a list.

    Databricks allows a group to own an object, so a team is a first-class owner
    here too: ownership that dies with an account is ownership nobody wants.
    """

    resource: str
    resource_id: str
    principal_kind: str
    principal_id: str


@dataclass(frozen=True)
class Identity:
    """The principals one caller is, as the grants name them."""

    user_id: Optional[str] = None
    username: Optional[str] = None
    team_id: Optional[str] = None


@dataclass(frozen=True)
class Decision:
    """Everything one access question answers, so a screen can explain itself.

    `level` alone cannot say *why*: "read, because the team was granted it on
    the parent folder" and "read, because you own the table" send a person to
    two different screens when they want to change it.
    """

    #: Whether any rule or owner names this securable or an ancestor of it.
    governed: bool
    #: The level this identity holds, or None for "governed and nothing left".
    level: Optional[str]
    #: True when the level comes from owning the securable or an ancestor.
    owned: bool
    #: The `kind/id` the winning rule was written on — the securable itself, or
    #: the ancestor it was inherited from.
    source: Optional[str]


def load(raw: Any) -> List[Grant]:
    """Reads the stored list, dropping anything that is not a well-formed grant.

    A malformed entry is discarded rather than guessed at: a half-read access
    rule is worse than a missing one, because it looks like a decision.
    """
    if not isinstance(raw, list):
        return []
    out: List[Grant] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        resource = str(item.get("resource") or "")
        level = str(item.get("level") or "")
        principal_kind = str(item.get("principalKind") or "")
        if resource not in RESOURCE_KINDS or level not in LEVEL_RANK:
            continue
        if principal_kind not in ("team", "user"):
            continue
        resource_id = str(item.get("resourceId") or "").strip()
        principal_id = str(item.get("principalId") or "").strip()
        if not resource_id or not principal_id:
            continue
        effect = "deny" if str(item.get("effect") or "allow").lower() == "deny" else "allow"
        out.append(
            Grant(
                resource=resource,
                resource_id=resource_id,
                principal_kind=principal_kind,
                principal_id=principal_id,
                level=level,
                effect=effect,
            )
        )
    return out


def load_owners(raw: Any) -> List[Owner]:
    """Reads the ownership records, dropping anything malformed.

    Accepts both shapes Studio can store: the list this writes back, and the
    `{"dataset:/lake/orders": {...}}` map a keyed record naturally becomes in
    JSON. One securable owns at most one entry; a duplicate keeps the first,
    because two owners is not a state the model has an answer for.
    """
    items: List[Any]
    if isinstance(raw, dict):
        items = list(raw.values())
    elif isinstance(raw, list):
        items = list(raw)
    else:
        return []

    out: List[Owner] = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        resource = str(item.get("resource") or "")
        principal_kind = str(item.get("principalKind") or "")
        if resource not in RESOURCE_KINDS or principal_kind not in ("team", "user"):
            continue
        resource_id = str(item.get("resourceId") or "").strip()
        principal_id = str(item.get("principalId") or "").strip()
        if not resource_id or not principal_id:
            continue
        key = (resource, resource_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            Owner(
                resource=resource,
                resource_id=resource_id,
                principal_kind=principal_kind,
                principal_id=principal_id,
            )
        )
    return out


def ancestors(resource_id: str) -> List[str]:
    """The containing addresses of one dataset, nearest first, without itself.

    `/lake/silver/orders` is contained by `/lake/silver` and by `/lake`, the way
    a table is contained by a schema and a catalog. `*` is not returned here: it
    is the wildcard every kind already has, and `scope_chain` appends it once.
    """
    trimmed = (resource_id or "").strip().rstrip("/")
    if not trimmed or trimmed == ANY:
        return []

    separator = next((sep for sep in _SEPARATORS if sep in trimmed), None)
    if separator is None:
        return []

    # Cut from the right rather than split and re-join: a `s3://bucket/...`
    # address has an empty segment in it that a round trip through `split` would
    # quietly drop, and `s3:/bucket` is not the same bucket.
    out: List[str] = []
    current = trimmed
    while True:
        head, found, _ = current.rpartition(separator)
        if not found:
            break
        head = head.rstrip(separator)
        # An empty head is the root of a path; one ending in `:` is the scheme of
        # a URL. Neither is a container anybody can grant on.
        if not head or head.endswith(":"):
            break
        out.append(head)
        current = head
    return out


def scope_chain(
    resource: str, resource_id: str, parents: Optional[Sequence[Tuple[str, str]]] = None
) -> List[Tuple[str, str]]:
    """Every `(kind, id)` a rule could be written on to reach this securable.

    Nearest first, which is the order a screen wants to read it in; evaluation
    itself does not depend on the order, because levels are cumulative and a
    deny anywhere wins.

    `parents` is how a Job says which Workflow it belongs to. The runner knows
    that and this module does not — a Job is a record in the workspace, and
    keeping the lookup out here is what stops this file from needing one.
    """
    chain: List[Tuple[str, str]] = []
    seen = set()

    def add(kind: str, ident: str) -> None:
        key = (kind, ident)
        if key not in seen:
            seen.add(key)
            chain.append(key)

    clean = (resource_id or "").strip()
    if clean:
        add(resource, clean)
    for ancestor in ancestors(clean):
        add(resource, ancestor)
    add(resource, ANY)

    for parent_kind, parent_id in parents or ():
        parent_clean = (parent_id or "").strip()
        if not parent_clean or parent_kind not in RESOURCE_KINDS:
            continue
        add(parent_kind, parent_clean)
        for ancestor in ancestors(parent_clean):
            add(parent_kind, ancestor)
        add(parent_kind, ANY)

    return chain


def _reaches(principal_kind: str, principal_id: str, identity: Identity) -> bool:
    if principal_id == ANY:
        return True
    if principal_kind == "team":
        return bool(identity.team_id) and principal_id == identity.team_id
    if identity.user_id and principal_id == identity.user_id:
        return True
    # A runner with no user records still has an identity — the shared token —
    # and no ids to match against, so the username stands in for one.
    return not identity.user_id and bool(identity.username) and principal_id == identity.username


def relevant(grants: Sequence[Grant], resource: str, resource_id: str) -> List[Grant]:
    return [
        grant
        for grant in grants
        if grant.resource == resource
        and (grant.resource_id == resource_id or grant.resource_id == ANY)
    ]


def owner_of(
    owners: Sequence[Owner],
    resource: str,
    resource_id: str,
) -> Optional[Owner]:
    """The owner recorded on this securable itself, ignoring inheritance."""
    clean = (resource_id or "").strip()
    for owner in owners:
        if owner.resource == resource and owner.resource_id == clean:
            return owner
    return None


def owns(
    owners: Sequence[Owner],
    resource: str,
    resource_id: str,
    identity: Identity,
    parents: Optional[Sequence[Tuple[str, str]]] = None,
) -> Optional[str]:
    """The `kind/id` this identity owns in the chain, or None.

    Owning the container is owning what is inside it: whoever owns a Workflow
    owns the Jobs in it, and whoever owns `/lake/silver` owns the tables under
    it. That is the rule that makes ownership usable — otherwise every new table
    would arrive unowned and need a second decision.
    """
    if not owners:
        return None
    for kind, ident in scope_chain(resource, resource_id, parents):
        for owner in owners:
            if owner.resource != kind or owner.resource_id != ident:
                continue
            if _reaches(owner.principal_kind, owner.principal_id, identity):
                return f"{kind}/{ident}"
    return None


def evaluate(
    grants: Sequence[Grant],
    owners: Sequence[Owner],
    resource: str,
    resource_id: str,
    identity: Identity,
    parents: Optional[Sequence[Tuple[str, str]]] = None,
) -> Decision:
    """The whole answer for one securable: governed, level, owner, and source.

    `governed` is separate from the level on purpose: "no rule" and "every rule
    refused" are different answers, and only the caller knows which default the
    first one deserves.
    """
    chain = scope_chain(resource, resource_id, parents)

    held = owns(owners, resource, resource_id, identity, parents)
    if held is not None:
        # Ownership is checked before the grants and not against them: the point
        # of an owner is that it is the one principal a rule cannot shut out.
        return Decision(governed=True, level="admin", owned=True, source=held)

    governed = any(
        owner.resource == kind and owner.resource_id == ident
        for kind, ident in chain
        for owner in owners
    )

    best = 0
    best_source: Optional[str] = None
    deny_rank = 0
    for kind, ident in chain:
        for grant in grants:
            if grant.resource != kind or grant.resource_id != ident:
                continue
            governed = True
            if not _reaches(grant.principal_kind, grant.principal_id, identity):
                continue
            rank = LEVEL_RANK[grant.level]
            if grant.effect == "deny":
                # A deny at `read` closes everything; a deny at `write` leaves reading.
                deny_rank = rank if deny_rank == 0 else min(deny_rank, rank)
            elif rank > best:
                best = rank
                best_source = f"{kind}/{ident}"

    if not governed:
        return Decision(governed=False, level=None, owned=False, source=None)
    if best == 0:
        return Decision(governed=True, level=None, owned=False, source=None)
    if deny_rank and deny_rank <= best:
        best = deny_rank - 1
    if best <= 0:
        return Decision(governed=True, level=None, owned=False, source=None)
    for name, rank in LEVEL_RANK.items():
        if rank == best:
            return Decision(governed=True, level=name, owned=False, source=best_source)
    return Decision(governed=True, level=None, owned=False, source=None)


def decide(
    grants: Sequence[Grant],
    resource: str,
    resource_id: str,
    identity: Identity,
    owners: Sequence[Owner] = (),
    parents: Optional[Sequence[Tuple[str, str]]] = None,
) -> Tuple[bool, Optional[str]]:
    """`(governed, level)` — the short form of `evaluate`, kept for callers that
    only need the answer and not the reason."""
    decision = evaluate(grants, owners, resource, resource_id, identity, parents)
    return decision.governed, decision.level


def allows(
    grants: Sequence[Grant],
    resource: str,
    resource_id: str,
    identity: Identity,
    level: str,
    owners: Sequence[Owner] = (),
    parents: Optional[Sequence[Tuple[str, str]]] = None,
) -> bool:
    """Whether this identity holds at least `level`. Ungoverned means yes."""
    decision = evaluate(grants, owners, resource, resource_id, identity, parents)
    if not decision.governed:
        return True
    return decision.level is not None and LEVEL_RANK[decision.level] >= LEVEL_RANK[level]


def may_administer(
    grants: Sequence[Grant],
    owners: Sequence[Owner],
    resource: str,
    resource_id: str,
    identity: Identity,
    parents: Optional[Sequence[Tuple[str, str]]] = None,
) -> bool:
    """Whether this identity may write the rules ON this securable.

    Databricks' answer, and the reason ownership is worth having: the owner of an
    object administers it without being an administrator of the platform. An
    `admin` grant on the object says the same thing — it is what a transfer of
    responsibility looks like before the transfer of ownership.
    """
    decision = evaluate(grants, owners, resource, resource_id, identity, parents)
    if decision.owned:
        return True
    return decision.governed and decision.level == "admin"


def refusal(resource: str, resource_id: str, level: str, username: str) -> str:
    """The message a refused caller gets. Says the rule, not just 'no'."""
    return (
        f"'{username or 'this caller'}' has no {level} access to the {resource} "
        f"{resource_id!r}. Access to it is restricted by the rules in the Studio "
        "catalog; its owner, or anyone holding admin on it, can grant a team or "
        "a user there."
    )


def ownership_refusal(resource: str, resource_id: str, username: str) -> str:
    """The message somebody gets for trying to change rules they do not own."""
    return (
        f"'{username or 'this caller'}' neither owns the {resource} "
        f"{resource_id!r} nor holds admin on it, so cannot change who may reach "
        "it. Ask its owner, or an administrator holding iam:ManageGrants."
    )


def of_kind(grants: Iterable[Grant], resource: str) -> List[Grant]:
    return [grant for grant in grants if grant.resource == resource]
