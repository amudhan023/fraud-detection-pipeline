.PHONY: up down demo logs build clean topic-list connector-status submit-jobs

COMPOSE       := docker compose -f infra/docker-compose.yml
FLINK_JAR     := flink-jobs/target/scala-2.12/fraud-pipeline-0.1.0-SNAPSHOT.jar
FLINK_REST    := http://localhost:8082

up:
	@echo "==> Building Flink image and starting full stack..."
	$(COMPOSE) build jobmanager taskmanager
	$(COMPOSE) up -d
	@echo "==> Waiting for Kafka Connect to be ready (may take ~60s)..."
	@until curl -sf http://localhost:8083/connectors > /dev/null 2>&1; do \
		printf '.'; sleep 3; done; echo ""
	@echo "==> Registering Debezium CDC connector..."
	envsubst < infra/kafka/connector-config.json | \
	  curl -sf -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @- \
	  && echo "Connector registered." \
	  || echo "Connector already exists or failed — check: make connector-status"
	@echo ""
	@echo "  Flink UI         → $(FLINK_REST)"
	@echo "  Grafana          → http://localhost:3000  (admin/admin)"
	@echo "  Prometheus       → http://localhost:9090"
	@echo "  Jaeger UI        → http://localhost:16686"
	@echo "  Kafka UI         → http://localhost:9080"
	@echo "  Kafka Connect    → http://localhost:8083"

down:
	$(COMPOSE) down -v --remove-orphans

build:
	@echo "==> Building Flink fat jar..."
	cd flink-jobs && sbt assembly
	@ls -lh flink-jobs/target/scala-2.12/*.jar

submit-jobs: build
	@echo "==> Submitting EnrichScore job..."
	curl -sf -X POST $(FLINK_REST)/jars/upload \
		-H "Expect:" -F "jarfile=@$(FLINK_JAR)"
	@JAR_ID=$$(curl -s $(FLINK_REST)/jars | python3 -c \
		"import sys,json; files=json.load(sys.stdin)['files']; print(files[-1]['id'])"); \
	echo "Jar ID: $$JAR_ID"; \
	curl -sf -X POST "$(FLINK_REST)/jars/$$JAR_ID/run" \
		-H "Content-Type: application/json" \
		-d '{"entryClass":"com.fraudpipeline.enrichment.EnrichScoreJob"}'; \
	curl -sf -X POST "$(FLINK_REST)/jars/$$JAR_ID/run" \
		-H "Content-Type: application/json" \
		-d '{"entryClass":"com.fraudpipeline.anomaly.AnomalyDetectionJob"}'; \
	curl -sf -X POST "$(FLINK_REST)/jars/$$JAR_ID/run" \
		-H "Content-Type: application/json" \
		-d '{"entryClass":"com.fraudpipeline.sinks.SpendAnalyticsJob"}'
	@echo "All three jobs submitted."

demo: up submit-jobs
	@echo "==> Starting load generator (500 txn/s, 5% fraud, 300s)..."
	cd load-generator && pip install -q -r requirements.txt && \
		python src/generator.py --rate 500 --fraud-pct 5 --duration 300

logs:
	$(COMPOSE) logs -f --tail=50

topic-list:
	$(COMPOSE) exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 --list

connector-status:
	@curl -s http://localhost:8083/connectors/transactions-cdc/status | python3 -m json.tool

clean:
	$(COMPOSE) down -v --remove-orphans
	cd flink-jobs && sbt clean
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
