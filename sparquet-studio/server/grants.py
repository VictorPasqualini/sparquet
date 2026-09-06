"""Access rules over the things the runner has no records for.

`auth.py` answers "may this person query at all". It cannot answer "may this
person query THIS table", because a dataset is not a record the runner owns — it
is an address a Job happens to mention, and the set of them only exists once
somebody walks the library. So Studio writes the rules and stores them beside
the catalog annotations, in `.studio/meta.json` under `grants`, and this module
evaluates them here, where the query actually runs. A rule the browser alone
honoured would protect nothing: `/query` is an HTTP endpoint, and curl is not
obliged to run the UI.

The two rules are `auth.py`'s, deliberately:

  1. A resource with NO rule naming it is open to whoever already holds the
     action. Anything else would break every runner that has never opened the
     permissions screen, and "somebody added a rule to an unrelated table and
     mine went dark" is not a failure mode worth having.
  2. An explicit deny beats every allow. A deny that can be widened away by
     adding another allow is not a deny.

Levels are cumulative: admin implies write implies read.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

#: Every resource kind a grant can name. Datasets are the reason this exists;
#: Jobs and Pipelines are here because the same question gets asked about them
#: and answering it in two places would let the two answers drift.
RESOURCE_KINDS = ("dataset", "job", "pipeline")

LEVELS = ("read", "write", "admin")

LEVEL_RANK: Dict[str, int] = {"read": 1, "write": 2, "admin": 3}

#: Both in the resource id and in the principal id.
ANY = "*"


@dataclass(frozen=True)
class Grant:
    resource: str
    resource_id: str
    principal_kind: str
    principal_id: str
    level: str
    effect: str


@dataclass(frozen=True)
class Identity:
    """The principals one caller is, as the grants name them."""

    user_id: Optional[str] = None
    username: Optional[str] = None
    team_id: Optional[str] = None


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


def _reaches(grant: Grant, identity: Identity) -> bool:
    if grant.principal_id == ANY:
        return True
    if grant.principal_kind == "team":
        return bool(identity.team_id) and grant.principal_id == identity.team_id
    if identity.user_id and grant.principal_id == identity.user_id:
        return True
    # A runner with no user records still has an identity — the shared token —
    # and no ids to match against, so the username stands in for one.
    return not identity.user_id and bool(identity.username) and grant.principal_id == identity.username


def relevant(grants: Sequence[Grant], resource: str, resource_id: str) -> List[Grant]:
    return [
        grant
        for grant in grants
        if grant.resource == resource
        and (grant.resource_id == resource_id or grant.resource_id == ANY)
    ]


def decide(
    grants: Sequence[Grant], resource: str, resource_id: str, identity: Identity
) -> Tuple[bool, Optional[str]]:
    """`(governed, level)` — whether any rule names it, and what it leaves.

    `governed` is separate from the level on purpose: "no rule" and "every rule
    refused" are different answers, and only the caller knows which default the
    first one deserves.
    """
    scoped = relevant(grants, resource, resource_id)
    if not scoped:
        return False, None

    best = 0
    deny_rank = 0
    for grant in scoped:
        if not _reaches(grant, identity):
            continue
        rank = LEVEL_RANK[grant.level]
        if grant.effect == "deny":
            # A deny at `read` closes everything; a deny at `write` leaves reading.
            deny_rank = rank if deny_rank == 0 else min(deny_rank, rank)
        elif rank > best:
            best = rank

    if best == 0:
        return True, None
    if deny_rank and deny_rank <= best:
        best = deny_rank - 1
    if best <= 0:
        return True, None
    for name, rank in LEVEL_RANK.items():
        if rank == best:
            return True, name
    return True, None


def allows(
    grants: Sequence[Grant],
    resource: str,
    resource_id: str,
    identity: Identity,
    level: str,
) -> bool:
    """Whether this identity holds at least `level`. Ungoverned means yes."""
    governed, held = decide(grants, resource, resource_id, identity)
    if not governed:
        return True
    return held is not None and LEVEL_RANK[held] >= LEVEL_RANK[level]


def refusal(resource: str, resource_id: str, level: str, username: str) -> str:
    """The message a refused caller gets. Says the rule, not just 'no'."""
    return (
        f"'{username or 'this caller'}' has no {level} access to the {resource} "
        f"{resource_id!r}. Access to it is restricted by the rules in the Studio "
        "catalog; whoever owns it can grant a team or a user there."
    )


def of_kind(grants: Iterable[Grant], resource: str) -> List[Grant]:
    return [grant for grant in grants if grant.resource == resource]
