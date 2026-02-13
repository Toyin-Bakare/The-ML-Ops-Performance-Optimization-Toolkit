# Project 4: Embedding Versioning + Safe Model Upgrades (Shadow Index + Promote/Rollback)

This demo shows a production-style embedding/index upgrade workflow:
- Store documents in a source-of-truth DB (SQLite)
- Build versioned embeddings + a FAISS index for each version (v1, v2, ...)
- Run shadow evaluation against a golden query set
- Promote the new version if metrics pass, else rollback

## Use Case
Embedding Versioning + Safe Model Upgrades (Shadow + Promote/Rollback)
Real-life use case

You want to upgrade embeddings (ex: text-embedding-3-small → newer model) to improve relevance.

But embedding upgrades can silently break retrieval.

So you:

build the new index in shadow

evaluate on a golden set

only promote if it improves metrics

rollback if it regresses

Why it matters

Prevents production regressions

Makes AI systems safer + more stable

Enables continuous improvement without downtime


## Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Seed demo docs and golden queries
python -m app.seed

## Build v1 and promote (baseline)
python -m app.cli build --version v1
python -m app.cli eval --version v1
python -m app.cli promote --version v1

## Build v2 in shadow + evaluate vs active
python -m app.cli build --version v2
python -m app.cli shadow-eval --candidate v2

## Promote v2 if it passes thresholds
python -m app.cli promote --version v2 --require-shadow-pass

## View active version
python -m app.cli active

## Run tests
pytest -q
