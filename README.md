# Gridworks Debug Cli

[![PyPI](https://img.shields.io/pypi/v/gridworks-debug-cli.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/gridworks-debug-cli.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/gridworks-debug-cli)][python version]
[![License](https://img.shields.io/pypi/l/gridworks-debug-cli)][license]

[![Read the documentation at https://gridworks-debug-cli.readthedocs.io/](https://img.shields.io/readthedocs/gridworks-debug-cli/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/thegridelectric/gridworks-debug-cli/workflows/Tests/badge.svg)][tests]
[![Codecov](https://codecov.io/gh/thegridelectric/gridworks-debug-cli/branch/main/graph/badge.svg)][codecov]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/gridworks-debug-cli/
[status]: https://pypi.org/project/gridworks-debug-cli/
[python version]: https://pypi.org/project/gridworks-debug-cli
[read the docs]: https://gridworks-debug-cli.readthedocs.io/
[tests]: https://github.com/anschweitzer/gridworks-debug-cli/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/anschweitzer/gridworks-debug-cli
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

Internal debugging tools for Gridworks systems:

```shell
brew install awscli
aws configure
pip install gridworks-debug-cli
gwd events mkconfig
gwd events show
```

This tool will be maintained only as long as it is internally useful. YMMV.

## Features

- Event viewing, either from local directory of events or from the cloud.

## Requirements

- [awscli](https://aws.amazon.com/cli/). This should be installable
  per [Amazon instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) or on a
  mac with:
  ```shell
  brew install awscli
  ```
- AWS credentials from Gridworks installed per
  [Amazon instructions](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) or:
  ```shell
  aws configure
  ```

## Installation

You can install _Gridworks Debug Cli_ via [pip] from [PyPI]:

Install awscli per per [Amazon instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
or on a mac with:

```shell
brew install awscli
```

Get AWS credentials from Gridworks and install them with:

```shell
aws configure
```

Install gridworks-debug-cli with:

```shell
pip install gridworks-debug-cli
```

Configure gridworks-debug-cli with:

```shell
gwd events mkconfig
open $HOME/.config/gridworks/debug-cli/events/gwd.events.config.json
```

You **must** fill in values for the following keys with information from Gridworks:

```json
{
  "mqtt": {
    "hostname": "USE REAL VALUE",
    "password": "USE REAL VALUE",
    "username": "USE REAL VALUE"
  },
  "sync": {
    "s3": {
      "bucket": "USE REAL VALUE",
      "prefix": "USE REAL VALUE",
      "profile": "USE NAME YOU CHOSE in 'aws configure'"
    }
  }
}
```

## Usage

Please see the [Command-line Reference] for details.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Gridworks Debug Cli_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/anschweitzer/gridworks-debug-cli/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/anschweitzer/gridworks-debug-cli/blob/main/LICENSE
[contributor guide]: https://github.com/anschweitzer/gridworks-debug-cli/blob/main/CONTRIBUTING.md
[command-line reference]: https://gridworks-debug-cli.readthedocs.io/en/latest/usage.html
