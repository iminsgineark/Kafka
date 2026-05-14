curl -X PUT http://localhost:8083/connectors/api-source-demo/config \
-H "Content-Type: application/json" \
-d '{
"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
"tasks.max":"2",
"file":"input.txt",
"topic":"api-topic"
}'
