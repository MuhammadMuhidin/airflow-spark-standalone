build :
	@docker build -t mumix/spark -f docker/dockerfile.spark .
	@docker build -t mumix/airflow -f docker/dockerfile.airflow .
	@docker build -t mumix/jupyter -f docker/dockerfile.jupyter .
	@docker network ls | grep -q mumix-network || docker network create mumix-network

up :
	@docker compose -f ./docker/docker_compose_spark.yml --env-file .env up -d
	@docker compose -f ./docker/docker_compose_airflow.yml --env-file .env up -d
	@docker compose -f ./docker/docker_compose_jupyter.yml --env-file .env up -d

down :
	@docker compose -f ./docker/docker_compose_spark.yml down
	@docker compose -f ./docker/docker_compose_airflow.yml down
	@docker compose -f ./docker/docker_compose_jupyter.yml down
