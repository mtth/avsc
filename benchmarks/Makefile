benchmark-avsc: | dat/user-100000.avro
	node avsc/benchmark.js

benchmark-java: | dat/user-100000.avro
	cd java && mvn compile >/dev/null && mvn -q exec:java -Dexec.mainClass=avsc.Avsc

benchmark-python: | dat/user-100000.avro
	python python/benchmark.py

dat/user-100000.avro: | dat/avro-tools.jar
	java -jar $| random --count 100000 --codec null --schema-file dat/user.avsc $@

dat/avro-tools.jar:
	curl http://mirror.nexcess.net/apache/avro/avro-1.7.7/java/avro-tools-1.7.7.jar >$@
