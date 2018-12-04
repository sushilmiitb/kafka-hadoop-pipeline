# Kafka - Hadoop Pipeline
Events are generated at the server while serving ads containing info about error reports, ad price, ad cost etc. Corresponding events are also generated at client's device when ad is being shown. They contain info like call to actions, interaction data, gaze params etc. 
These event pass through following pipeline-
## Impression Logger
Adserver generates serving logs into Kafka topic - response_logs. This module pre-processes that and puts into impressions topic
## Event Join
This module joins events generated at the ad server and clients device. 
## kconnect
This module consumes all the kafka topics and dumps it into HDFS.
## Aggregator
This module periodically reads the data stored in HDFS and converts it into meaningful data, such impression, cost and revenue metric for the ads served. It is then put into PostgreSQL database which then powers Advertiser and Publisher databases.