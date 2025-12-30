# CHANGELOG


## v1.1.2 (2025-12-30)

### Bug Fixes

- Normalize primary_keys values to match in drop_empty checks
  ([`2e6a012`](https://github.com/celine-eu/tap-spreadsheets/commit/2e6a0120c4151501266c102cd279cb014433f9f3))

### Chores

- Update version
  ([`81e135b`](https://github.com/celine-eu/tap-spreadsheets/commit/81e135b9974a52b12530e2730f5007b31ba76da3))


## v1.1.1 (2025-11-03)

### Bug Fixes

- Review ty errors, add dev deps
  ([`2b2d404`](https://github.com/celine-eu/tap-spreadsheets/commit/2b2d4046d47101b6c8e93e2f3edff7eb6d8936dc))

### Chores

- Fix typings
  ([`0022e6e`](https://github.com/celine-eu/tap-spreadsheets/commit/0022e6ec0d87daa17a5928b87bd92c1a40eedd8e))

- Fix typings
  ([`aba3f96`](https://github.com/celine-eu/tap-spreadsheets/commit/aba3f96dddaeb8f0fc3e2d6b48855ba022307b70))

- **deps**: Bump meltano from 3.9.1 to 4.0.1
  ([`1a703bb`](https://github.com/celine-eu/tap-spreadsheets/commit/1a703bb0c6abb370a4c678fae2be4eaf7653b06b))

Bumps [meltano](https://github.com/meltano/meltano) from 3.9.1 to 4.0.1. - [Release
  notes](https://github.com/meltano/meltano/releases) -
  [Changelog](https://github.com/meltano/meltano/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/meltano/meltano/compare/v3.9.1...v4.0.1)

--- updated-dependencies: - dependency-name: meltano dependency-version: 4.0.1

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Bump ty from 0.0.1a21 to 0.0.1a24
  ([`8e8bc6c`](https://github.com/celine-eu/tap-spreadsheets/commit/8e8bc6c9344a2d013a3cab120f5cc6d316f14ca3))

Bumps [ty](https://github.com/astral-sh/ty) from 0.0.1a21 to 0.0.1a24. - [Release
  notes](https://github.com/astral-sh/ty/releases) -
  [Changelog](https://github.com/astral-sh/ty/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/astral-sh/ty/compare/0.0.1-alpha.21...0.0.1-alpha.24)

--- updated-dependencies: - dependency-name: ty dependency-version: 0.0.1a24

dependency-type: direct:production

update-type: version-update:semver-patch ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Bump types-requests from 2.32.4.20250809 to 2.32.4.20250913
  ([`450a7d2`](https://github.com/celine-eu/tap-spreadsheets/commit/450a7d205dd41314684c9d38533ecf3d60b3d2c7))

Bumps [types-requests](https://github.com/typeshed-internal/stub_uploader) from 2.32.4.20250809 to
  2.32.4.20250913. - [Commits](https://github.com/typeshed-internal/stub_uploader/commits)

--- updated-dependencies: - dependency-name: types-requests dependency-version: 2.32.4.20250913

dependency-type: direct:production

update-type: version-update:semver-patch ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Update singer-sdk[faker] requirement
  ([`e8d992d`](https://github.com/celine-eu/tap-spreadsheets/commit/e8d992d37079b94ee11a42fbe778ab1070dab7df))

Updates the requirements on [singer-sdk[faker]](https://github.com/meltano/sdk) to permit the latest
  version. - [Release notes](https://github.com/meltano/sdk/releases) -
  [Changelog](https://github.com/meltano/sdk/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/meltano/sdk/compare/v0.49.1...v0.52.2)

--- updated-dependencies: - dependency-name: singer-sdk[faker] dependency-version: 0.52.2

dependency-type: direct:production ...

Signed-off-by: dependabot[bot] <support@github.com>

### Continuous Integration

- Bump the actions group across 1 directory with 3 updates
  ([`56f5a94`](https://github.com/celine-eu/tap-spreadsheets/commit/56f5a944c14dc9e7ddfc84c374a1e885e50fea3a))

Bumps the actions group with 3 updates in the / directory:
  [hynek/build-and-inspect-python-package](https://github.com/hynek/build-and-inspect-python-package),
  [actions/download-artifact](https://github.com/actions/download-artifact) and
  [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv).

Updates `hynek/build-and-inspect-python-package` from 2.13.0 to 2.14.0 - [Release
  notes](https://github.com/hynek/build-and-inspect-python-package/releases) -
  [Changelog](https://github.com/hynek/build-and-inspect-python-package/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/hynek/build-and-inspect-python-package/compare/c52c3a4710070b50470d903818a7b25115dcd076...efb823f52190ad02594531168b7a2d5790e66516)

Updates `actions/download-artifact` from 5.0.0 to 6.0.0 - [Release
  notes](https://github.com/actions/download-artifact/releases) -
  [Commits](https://github.com/actions/download-artifact/compare/634f93cb2916e3fdff6788551b99b062d0335ce0...018cc2cf5baa6db3ef3c5f8a56943fffe632ef53)

Updates `astral-sh/setup-uv` from 6.7.0 to 7.1.2 - [Release
  notes](https://github.com/astral-sh/setup-uv/releases) -
  [Commits](https://github.com/astral-sh/setup-uv/compare/b75a909f75acd358c2196fb9a5f1299a9a8868a4...85856786d1ce8acfbcc2f13a5f3fbd6b938f9f41)

--- updated-dependencies: - dependency-name: hynek/build-and-inspect-python-package
  dependency-version: 2.14.0

dependency-type: direct:production

update-type: version-update:semver-minor

dependency-group: actions

- dependency-name: actions/download-artifact dependency-version: 6.0.0

update-type: version-update:semver-major

- dependency-name: astral-sh/setup-uv dependency-version: 7.1.2

dependency-group: actions ...

Signed-off-by: dependabot[bot] <support@github.com>


## v1.1.0 (2025-11-03)

### Bug Fixes

- Improve s3 path handlng
  ([`00511d7`](https://github.com/celine-eu/tap-spreadsheets/commit/00511d7844aafc4c29fa9758c1e1a9ae1a6d1ea0))

- Support dataclass
  ([`6ee947d`](https://github.com/celine-eu/tap-spreadsheets/commit/6ee947dafe4b83a03c9e04c583a49628ef4ac5db))

- Use LastModified from S3 storage
  ([`4b2a1e5`](https://github.com/celine-eu/tap-spreadsheets/commit/4b2a1e5711b1c56bdff382930700c3a1c56380b9))

### Chores

- Add semantic release
  ([`cecd07f`](https://github.com/celine-eu/tap-spreadsheets/commit/cecd07f9d5b47859d571cc470314bd89d14f7a35))

### Features

- Add s3 test
  ([`3803591`](https://github.com/celine-eu/tap-spreadsheets/commit/38035914010fb098788c35af9c96303e89943adc))


## v1.0.5 (2025-10-06)


## v1.0.4 (2025-10-05)

### Chores

- **deps**: Bump mypy in the runtime-dependencies group
  ([`a9002a8`](https://github.com/celine-eu/tap-spreadsheets/commit/a9002a83c3c914e412e35e1c29893b5a1ab6e8a6))

Bumps the runtime-dependencies group with 1 update: [mypy](https://github.com/python/mypy).

Updates `mypy` from 1.18.1 to 1.18.2 -
  [Changelog](https://github.com/python/mypy/blob/master/CHANGELOG.md) -
  [Commits](https://github.com/python/mypy/compare/v1.18.1...v1.18.2)

--- updated-dependencies: - dependency-name: mypy dependency-version: 1.18.2

dependency-type: direct:production

update-type: version-update:semver-patch

dependency-group: runtime-dependencies ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Bump ty from 0.0.1a20 to 0.0.1a21
  ([`429daa8`](https://github.com/celine-eu/tap-spreadsheets/commit/429daa83b7bb6e21113b71c9538576dcf6d3324a))

Bumps [ty](https://github.com/astral-sh/ty) from 0.0.1a20 to 0.0.1a21. - [Release
  notes](https://github.com/astral-sh/ty/releases) -
  [Changelog](https://github.com/astral-sh/ty/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/astral-sh/ty/compare/0.0.1-alpha.20...0.0.1-alpha.21)

--- updated-dependencies: - dependency-name: ty dependency-version: 0.0.1a21

dependency-type: direct:production

update-type: version-update:semver-patch ...

Signed-off-by: dependabot[bot] <support@github.com>


## v1.0.3 (2025-10-05)


## v1.0.2 (2025-10-05)


## v1.0.1 (2025-09-18)


## v1.0.0 (2025-09-18)

### Chores

- **deps**: Bump mypy from 1.17.1 to 1.18.1
  ([`5bb3665`](https://github.com/celine-eu/tap-spreadsheets/commit/5bb3665ad6dd4c1cc9d70050fe7871c7913009ec))

Bumps [mypy](https://github.com/python/mypy) from 1.17.1 to 1.18.1. -
  [Changelog](https://github.com/python/mypy/blob/master/CHANGELOG.md) -
  [Commits](https://github.com/python/mypy/compare/v1.17.1...v1.18.1)

--- updated-dependencies: - dependency-name: mypy dependency-version: 1.18.1

dependency-type: direct:production

update-type: version-update:semver-minor ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Bump s3fs from 2025.7.0 to 2025.9.0
  ([`056413c`](https://github.com/celine-eu/tap-spreadsheets/commit/056413cfe49659d04976f87650ee2289fc59fe09))

Bumps [s3fs](https://github.com/fsspec/s3fs) from 2025.7.0 to 2025.9.0. -
  [Changelog](https://github.com/fsspec/s3fs/blob/main/release-procedure.md) -
  [Commits](https://github.com/fsspec/s3fs/compare/2025.7.0...2025.9.0)

--- updated-dependencies: - dependency-name: s3fs dependency-version: 2025.9.0

dependency-type: direct:production

update-type: version-update:semver-minor ...

Signed-off-by: dependabot[bot] <support@github.com>

- **deps**: Update singer-sdk[faker] requirement
  ([`69cc8c7`](https://github.com/celine-eu/tap-spreadsheets/commit/69cc8c7e5bf395a8bf2d67cc112fceeb5a56d8b7))

Updates the requirements on [singer-sdk[faker]](https://github.com/meltano/sdk) to permit the latest
  version. - [Release notes](https://github.com/meltano/sdk/releases) -
  [Changelog](https://github.com/meltano/sdk/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/meltano/sdk/compare/v0.48.1...v0.49.1)

--- updated-dependencies: - dependency-name: singer-sdk[faker] dependency-version: 0.49.1

dependency-type: direct:production ...

Signed-off-by: dependabot[bot] <support@github.com>

### Continuous Integration

- Bump astral-sh/setup-uv from 6.6.1 to 6.7.0 in the actions group
  ([`2da6c14`](https://github.com/celine-eu/tap-spreadsheets/commit/2da6c14b0f75c2b2f67488ecd0178c547344ffe3))

Bumps the actions group with 1 update: [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv).

Updates `astral-sh/setup-uv` from 6.6.1 to 6.7.0 - [Release
  notes](https://github.com/astral-sh/setup-uv/releases) -
  [Commits](https://github.com/astral-sh/setup-uv/compare/557e51de59eb14aaaba2ed9621916900a91d50c6...b75a909f75acd358c2196fb9a5f1299a9a8868a4)

--- updated-dependencies: - dependency-name: astral-sh/setup-uv dependency-version: 6.7.0

dependency-type: direct:production

update-type: version-update:semver-minor

dependency-group: actions ...

Signed-off-by: dependabot[bot] <support@github.com>

- Bump the actions group with 3 updates
  ([`555d9d6`](https://github.com/celine-eu/tap-spreadsheets/commit/555d9d660ae51572fe216ae4f45030d865064363))

Bumps the actions group with 3 updates:
  [pypa/gh-action-pypi-publish](https://github.com/pypa/gh-action-pypi-publish),
  [actions/setup-python](https://github.com/actions/setup-python) and
  [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv).

Updates `pypa/gh-action-pypi-publish` from 1.12.4 to 1.13.0 - [Release
  notes](https://github.com/pypa/gh-action-pypi-publish/releases) -
  [Commits](https://github.com/pypa/gh-action-pypi-publish/compare/76f52bc884231f62b9a034ebfe128415bbaabdfc...ed0c53931b1dc9bd32cbe73a98c7f6766f8a527e)

Updates `actions/setup-python` from 5.6.0 to 6.0.0 - [Release
  notes](https://github.com/actions/setup-python/releases) -
  [Commits](https://github.com/actions/setup-python/compare/a26af69be951a213d495a4c3e4e4022e16d87065...e797f83bcb11b83ae66e0230d6156d7c80228e7c)

Updates `astral-sh/setup-uv` from 6.6.0 to 6.6.1 - [Release
  notes](https://github.com/astral-sh/setup-uv/releases) -
  [Commits](https://github.com/astral-sh/setup-uv/compare/4959332f0f014c5280e7eac8b70c90cb574c9f9b...557e51de59eb14aaaba2ed9621916900a91d50c6)

--- updated-dependencies: - dependency-name: pypa/gh-action-pypi-publish dependency-version: 1.13.0

dependency-type: direct:production

update-type: version-update:semver-minor

dependency-group: actions

- dependency-name: actions/setup-python dependency-version: 6.0.0

update-type: version-update:semver-major

- dependency-name: astral-sh/setup-uv dependency-version: 6.6.1

update-type: version-update:semver-patch

dependency-group: actions ...

Signed-off-by: dependabot[bot] <support@github.com>
