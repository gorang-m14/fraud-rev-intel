.PHONY: up down seed dbt sync logs

up:
	docker compose up -d --build

down:
	docker compose down -v

seed:
	docker compose run --rm generator python -m ingestion.generate --rows 5000

dbt:
	docker compose run --rm dbt dbt deps
	docker compose run --rm dbt dbt build

sync:
	docker compose run --rm generator python -m warehouse.sync_to_clickhouse

logs:
	docker compose logs -f --tail=200
