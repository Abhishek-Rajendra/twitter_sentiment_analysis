# twitter_sentiment_analysis

Documents/apps/kafka/bin/zookeeper-server-start.sh ./Documents/apps/kafka/config/zookeeper.properties

Documents/apps/kafka/bin/kafka-server-start.sh ./Documents/apps/kafka/server.properties

bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic basic

kafka-console-producer --bootstrap-server localhost:9092 --topic basic

kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic basic


sudo systemctl enable elasticsearch

sudo systemctl enable kibana

http://localhost:5601/app/home


curl --cacert /etc/elasticsearch/certs/http_ca.crt -u elastic:y31tB4rL8+YdxZWOsSR0 https://localhost:9200 

curl -X POST "https://api.twitter.com/2/tweets/search/stream/rules" -H "Authorization: Bearer AAAAAAAAAAAAAAAAAAAAAK1ybgEAAAAAJ18tuG7POzsOYYXvN85EgbR%2FTHA%3DnZVppVqUrg2KqJaoujDUa3q0WHJdvyAv72lTRfcv2u78D9JcsP" -H "Content-type: application/json" -d '{"add":[{"value":"#kgf2"}]}'

