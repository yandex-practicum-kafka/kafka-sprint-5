#

docker compose exec kafka-0 bash

#

export BOOTSTRAP_SERVERS="kafka-0:9093"
export COMMAND_CONFIG="/bitnami/kafka/config/certs/client.properties"

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --list

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --list

#

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --create --topic topic-1 --partitions 3 --replication-factor 3
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --create --topic topic-2 --partitions 3 --replication-factor 3

kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --list

#

User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU

#

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation Write --topic topic-1
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation Write --topic topic-2
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation Read --topic topic-1
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation Describe --topic topic-1
kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation Describe --topic topic-2

#

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal User:1.2.840.113549.1.9.1="#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation READ --topic __consumer_offsets

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal User:1.2.840.113549.1.9.1="#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation WRITE --topic __consumer_offsets

#

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU --operation DESCRIBE --group group_id

#

kafka-acls.sh --bootstrap-server $BOOTSTRAP_SERVERS --command-config $COMMAND_CONFIG --add --allow-principal "User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU" --operation READ --group group_id