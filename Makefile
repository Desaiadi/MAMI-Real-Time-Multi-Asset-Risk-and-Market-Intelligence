.PHONY: install run clean

install:
	pip install -r requirements.txt

run:
	python run.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
