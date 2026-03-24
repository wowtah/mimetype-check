set windows-shell := ["powershell", "-NoLogo", "-Command"]

default:
    @just --list

# Run the Kafka file-head analyzer
run *ARGS:
    uv run kafka-file-head-analyzer {{ARGS}}

# Run all QA checks: format, lint, fix, and typecheck
qa: fmt lint typecheck

# Auto-format with ruff
fmt:
    uv run ruff format .

# Lint and auto-fix with ruff
lint:
    uv run ruff check --fix .

# Type-check with pyright
typecheck:
    uv run python -m pyright

# Sync all deps (including dev)
sync:
    uv sync --all-extras
