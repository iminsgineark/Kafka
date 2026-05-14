curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d '{
"name":"api-source-demo",
"config":{
"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
"tasks.max":"1",
"file":"input.txt",
"topic":"api-topic"
}
}'
