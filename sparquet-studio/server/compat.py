"""Which versions of the `sparquet` framework this Studio speaks to.

Inside the repository this question never comes up: the Studio and the framework
are edited in the same commit, so they cannot disagree. The moment the Studio
lives in a repository of its own, they can — somebody installs `sparquet` from
PyPI, the Studio is a version older or newer, and the symptom is a compiled Job
that the framework rejects for a reason neither side explains.

So the supported range is declared here, once, and everything else reads it:
`server/requirements.txt` pins the same range for an install, `/health` reports
whether the installed framework falls inside it, and the Studio shows the
mismatch instead of letting it surface later as a run that fails oddly.

The range is deliberately a range and not an exact pin. A patch of the framework
must not require a release of the Studio, and the compiler is already held to the
framework by the round-trip tests over `examples/` — those are what would go red
if a minor release changed the JSON, which is the signal that moves this ceiling.

There is no dependency on `packaging` for this. The runner's requirements are
three packages and a note about why they are three, and a version comparison of
the form `0.12.3` is a tuple of integers.
"""
from __future__ import annotations

import re
from typing import NamedTuple, Optional, Tuple

#: The floor: the oldest framework whose JSON this Studio compiles correctly.
#: Raise it when the Studio starts emitting something an older framework cannot
#: read — not when a new framework gains a feature the Studio does not use yet.
MINIMUM = (0, 12)

#: The ceiling, exclusive. The next minor is assumed incompatible until somebody
#: has run the round-trip suite against it, because that suite is the only thing
#: that actually knows.
BELOW = (0, 13)

#: What an install should ask for. Kept in step with the two tuples above by
#: `test_compat.py`, so the requirements file and the runtime check cannot drift.
REQUIREMENT = "sparquet>=0.12,<0.13"

_VERSION = re.compile(r"^\s*v?(\d+)\.(\d+)(?:\.(\d+))?")


def parse(version: Optional[str]) -> Optional[Tuple[int, int, int]]:
    """`"0.12.1"` as `(0, 12, 1)`, or None when it is not a version at all.

    Anything after the third number — `rc1`, `+local`, `.post2` — is ignored on
    purpose: a pre-release of 0.12 is treated as 0.12, which is what somebody
    testing one wants, and the alternative is a full PEP 440 parser for a
    question that never needed one.
    """
    if not version:
        return None
    match = _VERSION.match(version)
    if not match:
        return None
    major, minor, patch = match.group(1), match.group(2), match.group(3)
    return (int(major), int(minor), int(patch or 0))


class Compatibility(NamedTuple):
    #: False only when the framework is present AND outside the range. An absent
    #: framework is a different problem, reported by `spark_available` and by the
    #: import error, and calling it "incompatible" would send somebody looking
    #: for the wrong fix.
    supported: bool
    #: A sentence for a person, or None when there is nothing to say.
    message: Optional[str]
    #: The range, so the Studio can show it without hard-coding it too.
    requirement: str = REQUIREMENT


def check(version: Optional[str]) -> Compatibility:
    parsed = parse(version)
    if parsed is None:
        # No framework, or a version string this does not recognise. Either way
        # there is nothing to compare, and refusing to run on the basis of a
        # string nobody could parse would be worse than staying quiet.
        return Compatibility(True, None)

    found = parsed[:2]
    if found < MINIMUM:
        return Compatibility(
            False,
            f"This Studio needs {REQUIREMENT}, and the installed framework is "
            f"{version}. A Job compiled here may use fields that version does not "
            f"read. Upgrade with: pip install -U '{REQUIREMENT}'",
        )
    if found >= BELOW:
        return Compatibility(
            False,
            f"This Studio was built against {REQUIREMENT}, and the installed "
            f"framework is {version}. It will probably work, but nothing has "
            f"proven that this Studio still compiles the JSON that version "
            f"executes — update the Studio, or pin the framework with: "
            f"pip install '{REQUIREMENT}'",
        )
    return Compatibility(True, None)
