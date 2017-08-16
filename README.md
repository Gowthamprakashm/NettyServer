# NettyServer

STEPS FOLLOWED :

1)Setup Netty Server
2)Accept json from client
3)To Convert Json to Protobuf
	1)To convert Json to protobuf, write obj definition in .proto file format which matches the json.
	2)Compile the .proto file to get equivalent .java file
4)Setup Kafka
	1)Run Zookeeper Server
	2)Start Kafka Broker
	3)Create topic
	4)create consumer to check if working
5)Create producer
6)Create a object against auto generated protobuf class using recived json
	1)To acheive this,create a custom serializer class
	2)change the config prop as per this..
7)If successfully converted,push the obj as byte array to the kafka broker.
