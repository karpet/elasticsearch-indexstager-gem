test:
	ES_PATH_CONF=/tmp/es-config/ PATH=/usr/share/elasticsearch/bin/:${PATH} rake test

travis:
	rake test

.PHONY: test travis
