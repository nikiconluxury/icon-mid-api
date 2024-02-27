install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

format:
	black *.py mylib/*.py

lint:
	pylint --disable=R,C *.py mylib/*.py tasks/*.py

test:
	python -m pytest -vv --cov=mylib --cov=tasks --cov=main  test_*.py

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

all: install lint test build
