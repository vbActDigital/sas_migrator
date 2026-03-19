.PHONY: install mock test discover migrate pdf docker clean

install:
	pip install -r requirements.txt

install-full:
	pip install -r requirements.txt
	pip install pyreadstat sas7bdat pandas numpy openai

mock:
	python tests/create_mock_environment.py

test:
	python -m pytest tests/ -v

discover:
	python -m src discover --config config/snowflake_aws_config.yaml --out output --catalog --pdf

migrate:
	python -m src migrate --inventory output/inventory.json --config config/snowflake_aws_config.yaml --out output/migration

pdf:
	python tests/generate_mvp1_pdf.py

docker:
	docker compose up --build

docker-shell:
	docker compose run --rm sas-migrator bash

lint:
	python -m flake8 src/ tests/ --max-line-length 120

clean:
	rm -rf tests/mock_sas_environment tests/output output __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
