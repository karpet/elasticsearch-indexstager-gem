test6:
	PATH=/tmp/elasticsearch-6.0.0/bin:${PATH} rake test

test5:
	PATH=/tmp/elasticsearch-5.0.0/bin:${PATH} rake test

test2:
	PATH=/tmp/elasticsearch-2.4.6/bin:${PATH} rake test

test: test6

dist:
	gem build elasticsearch-indexstager.gemspec

setup:
	bundle install

.PHONY: test test5 test6 test2 dist
