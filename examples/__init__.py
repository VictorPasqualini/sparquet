"""The example configs, as a package so they travel inside the wheel.

There is no code here and there never should be. The file exists because the
example pipelines are not only documentation: they are the fixtures that pin the
Studio's compiler to the JSON this framework actually executes, and a fixture
that only exists in a git checkout cannot pin anything for somebody who installed
`sparquet` from PyPI.

`pyproject.toml` maps this directory onto `sparquet.examples`, so an installed
package carries the same `.json` files the repository does, and
`sparquet.examples_path()` finds them either way.
"""
