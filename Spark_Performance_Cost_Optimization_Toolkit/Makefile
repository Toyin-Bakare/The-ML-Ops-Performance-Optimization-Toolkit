install:
	python -m pip install -r requirements.txt

test:
	pytest -q

sample-report:
	python -m spark_opt.cli report --eventlog samples/sample_eventlog.jsonl --spark-conf samples/sample_spark_conf.json --out reports/report.md
