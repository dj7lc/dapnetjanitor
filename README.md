# DAPNET JANITOR Service

- reads the databases and checks for broken documents
- removes expired information
- reports problems

## Installation

It is recommended to use a Python virtual environment. First, create a new virtual environment for the project if not yet done:

    cd <project directory>
    python -m venv .venv

Next, activate the environment. For Windows, use

    .venv\Scripts\activate.bat

For Linux (bash/zsh), use

    source .venv/bin/activate


Next, install the requirements:

    python -m pip install -r requirements.txt

## Run megalinter locally

    docker run -e DEFAULT_WORKSPACE=/build -e MEGALINTER_CONFIG=.github/linters/mega-linter.yml -v `pwd`:/build -w /build oxsecurity/megalinter-python:v7
