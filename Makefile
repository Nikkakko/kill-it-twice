.PHONY: install dev up down seed verify build

install:
	pnpm install

up:
	docker compose up --build -d

down:
	docker compose down

dev:
	pnpm dev

build:
	pnpm build

seed:
	curl --fail-with-body -X POST http://localhost:3000/api/simulation/seed -H 'content-type: application/json' -d '{"count":20000,"reset":true}'

verify:
	./scripts/verify.sh
