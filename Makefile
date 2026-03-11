.PHONY: install mock test-mvp1 test lint clean

install:
	pip install -r requirements.txt

mock:
	python tests/create_mock_environment.py

test-mvp1:
	python tests/run_mvp1_test.py

test:
	python -m pytest tests/ -v

lint:
	python -m flake8 src/ tests/

clean:
	rm -rf tests/mock_sas_environment tests/output __pycache__ .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
