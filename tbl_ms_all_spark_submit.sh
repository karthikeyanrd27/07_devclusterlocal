#! /bin/sh
Users/KarthikeyanDurairaj/Desktop/sparklatest0802/bin/spark-submit --jars /Users/KarthikeyanDurairaj/jarfiles/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar --packages datastax:spark-cassandra-connector:2.3.0-s_2.11 tbl_ms_advertiser_compacttopic.py > tbl_ms_advertiser_log;
/Users/KarthikeyanDurairaj/Desktop/sparklatest0802/bin/spark-submit --jars /Users/KarthikeyanDurairaj/jarfiles/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar --packages datastax:spark-cassandra-connector:2.3.0-s_2.11 tbl_ms_property_compacttopic.py > tbl_ms_property_log ;
