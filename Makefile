init:
	uv sync

test:
	uv run pytest

web:
	uv run feedz-web

.PHONY: init test web
