#!/bin/bash

java -Xms24g -Xmx24g -cp kvs.jar:webserver.jar:log4j_lib/*:lucene_lib/*:tika_lib/* cis5550.kvs.Coordinator 8000

