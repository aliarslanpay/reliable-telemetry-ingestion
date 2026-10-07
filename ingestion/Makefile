.PHONY: build-IngestFunction
build-IngestFunction:
	mkdir -p "$(ARTIFACTS_DIR)/telemetry_ingestion"
	cp telemetry_ingestion/__init__.py telemetry_ingestion/core.py telemetry_ingestion/dynamo.py telemetry_ingestion/aws_handler.py "$(ARTIFACTS_DIR)/telemetry_ingestion/"
	python3 -m pip install --disable-pip-version-check -r requirements-cloud.txt --target "$(ARTIFACTS_DIR)"
