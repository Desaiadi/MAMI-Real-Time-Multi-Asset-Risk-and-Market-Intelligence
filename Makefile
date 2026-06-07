.PHONY: up down logs ps scale test test-deps clean

# ── Run the stack ───────────────────────────────────────
up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

# Scale a compute service, e.g.  make scale SVC=risk-engine N=3
scale:
	docker compose up -d --scale $(SVC)=$(N)

# ── Tests (run the shared algorithms without Docker) ────
test-deps:
	pip install -r requirements.txt

test:
	pytest

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
