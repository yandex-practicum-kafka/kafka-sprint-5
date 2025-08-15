#!/bin/sh
set -eu

log() { printf '%s | %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# bootstrap: взять из BOOTSTRAP_SERVERS или из KAFKA_CFG_BOOTSTRAP_SERVERS (если задано в compose)
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-${KAFKA_CFG_BOOTSTRAP_SERVERS:-kafka-0:9093}}"
# COMMAND_CONFIG может быть передан явно; если нет — автодетект
COMMAND_CONFIG="${COMMAND_CONFIG:-}"
PRINCIPAL="${1:-User:1.2.840.113549.1.9.1=#161a6b61666b615f75736572406f7267616e697a6174696f6e2e7275,CN=kafka_user,L=Locality,OU=OrganizationalUnit,O=Organization,C=RU}"
GROUP_ID="${GROUP_ID:-group_id}"

log "BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS"
log "COMMAND_CONFIG(incoming)=${COMMAND_CONFIG:-<none>}"
log "PRINCIPAL=$PRINCIPAL"
log "GROUP_ID=$GROUP_ID"

# Найти исполняемые kafka-утилиты в PATH или стандартных местах
find_exec() {
  name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  # типичные места для Bitnami / Confluent / upstream
  for p in /opt/bitnami/kafka/bin/"$name" /opt/kafka/bin/"$name" /usr/bin/"$name" /usr/local/bin/"$name"; do
    if [ -x "$p" ]; then
      printf '%s\n' "$p"
      return 0
    fi
  done
  return 1
}

KAFKA_TOPICS="$(find_exec kafka-topics.sh || true)"
KAFKA_ACLS="$(find_exec kafka-acls.sh || true)"

if [ -z "$KAFKA_TOPICS" ] || [ -z "$KAFKA_ACLS" ]; then
  log "ERROR: kafka-topics.sh or kafka-acls.sh not found. Searched PATH and common locations."
  log "kafka-topics: $KAFKA_TOPICS"
  log "kafka-acls:   $KAFKA_ACLS"
  log "Inspect image (e.g. docker compose exec kafka-0 sh -c 'ls -l /opt/bitnami/kafka/bin') and update script if necessary"
  exit 1
fi

log "Using: kafka-topics='$KAFKA_TOPICS', kafka-acls='$KAFKA_ACLS'"

# Автодетект client.properties или генерация временного из env KAFKA_CFG_*
select_or_gen_config() {
  # если явно задан и файл существует — используем
  if [ -n "$COMMAND_CONFIG" ] && [ -f "$COMMAND_CONFIG" ]; then
    printf '%s' "$COMMAND_CONFIG"
    return 0
  fi

  # проверяем обычные места (монтированные директории /certs-*)
  for c in /certs-0/client.properties /certs-1/client.properties /certs-2/client.properties /bitnami/kafka/config/certs/client.properties; do
    if [ -f "$c" ]; then
      printf '%s' "$c"
      return 0
    fi
  done

  # Если нет client.properties, попробуем сгенерировать временный на основе переменных окружения:
  # Ожидаемые переменные (в compose вы их передаёте): KAFKA_CFG_SSL_TRUSTSTORE_LOCATION, KAFKA_CFG_SSL_TRUSTSTORE_PASSWORD,
  # KAFKA_CFG_SSL_KEYSTORE_LOCATION, KAFKA_CFG_SSL_KEYSTORE_PASSWORD, KAFKA_CFG_SSL_KEY_PASSWORD, KAFKA_CFG_SECURITY_PROTOCOL
  if [ -n "${KAFKA_CFG_SSL_TRUSTSTORE_LOCATION:-}" ] && [ -n "${KAFKA_CFG_SSL_TRUSTSTORE_PASSWORD:-}" ]; then
    TS="${KAFKA_CFG_SSL_TRUSTSTORE_LOCATION}"
    KS="${KAFKA_CFG_SSL_KEYSTORE_LOCATION:-}"
    TSPASS="${KAFKA_CFG_SSL_TRUSTSTORE_PASSWORD}"
    KSPASS="${KAFKA_CFG_SSL_KEYSTORE_PASSWORD:-}"
    KEYPASS="${KAFKA_CFG_SSL_KEY_PASSWORD:-}"
    TMP_CONF="/tmp/ksetup-client.properties"
    {
      echo "security.protocol=${KAFKA_CFG_SECURITY_PROTOCOL:-SSL}"
      echo "ssl.truststore.location=$TS"
      echo "ssl.truststore.password=$TSPASS"
      [ -n "$KS" ] && echo "ssl.keystore.location=$KS"
      [ -n "$KSPASS" ] && echo "ssl.keystore.password=$KSPASS"
      [ -n "$KEYPASS" ] && echo "ssl.key.password=$KEYPASS"
      echo "ssl.endpoint.identification.algorithm="
    } > "$TMP_CONF"
    # проверка наличия файлов truststore/keystore
    if [ ! -f "$TS" ]; then
      log "ERROR: truststore not found at $TS (needed to generate client.properties)"
      return 1
    fi
    if [ -n "$KS" ] && [ ! -f "$KS" ]; then
      log "ERROR: keystore not found at $KS (needed to generate client.properties)"
      return 1
    fi
    log "Generated temporary client.properties at $TMP_CONF"
    printf '%s' "$TMP_CONF"
    return 0
  fi

  # Ничего не найдено
  return 1
}

USED_CONFIG="$(select_or_gen_config || true)"
if [ -z "$USED_CONFIG" ]; then
  log "ERROR: client.properties not found and could not be generated. Set COMMAND_CONFIG env to point to a valid file (e.g. /certs-0/client.properties)."
  exit 2
fi

log "Using client config: $USED_CONFIG"

# Ждать пока Kafka не станет доступен (показываем первые stderr для диагностики)
TIMEOUT=${TIMEOUT:-600}
SLEEP=${SLEEP:-5}
elapsed=0
probe=1
firsterr=1
while true; do
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --list >/dev/null 2>/dev/null; then
    log "Kafka is reachable"
    break
  fi
  # вывести stderr первого failed attempt для понимания причины
  if [ "$firsterr" -eq 1 ]; then
    log "First failure output (stderr) for debug:"
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --list 2>/tmp/kterr || true
    sed -n '1,200p' /tmp/kterr || true
    firsterr=0
  fi
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    log "ERROR: Kafka did not become ready within ${TIMEOUT}s"
    exit 3
  fi
  log "Kafka not ready yet (probe ${probe}/$((TIMEOUT / SLEEP))), retrying in ${SLEEP}s..."
  sleep "$SLEEP"
  elapsed=$((elapsed + SLEEP))
  probe=$((probe + 1))
done

# Создать топик, если нет
create_topic_if_missing() {
  topic="$1"; parts="$2"; rf="$3"
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --describe --topic "$topic" >/dev/null 2>&1; then
    log "Topic '$topic' exists — skip"
  else
    log "Creating topic '$topic' (partitions=$parts, replication=$rf)"
    if ! "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --create --topic "$topic" --partitions "$parts" --replication-factor "$rf"; then
      log "Warning: create topic '$topic' failed (may already exist or transient error)"
    fi
  fi
}

# Добавить ACL (не падаем при ошибке)
add_acl_safely() {
  log "Adding ACL: $"
  if ! "$KAFKA_ACLS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --add --allow-principal "$PRINCIPAL" "$@"; then
    log "Warning: adding ACL ($) failed (may already exist or transient error)"
  else
    log "ACL added: $*"
  fi
}

log "Creating topics..."
create_topic_if_missing topic-1 3 3
create_topic_if_missing topic-2 3 3

log "Applying ACLs for $PRINCIPAL"
add_acl_safely --operation Write --topic topic-1
add_acl_safely --operation Write --topic topic-2
add_acl_safely --operation Read  --topic topic-1
add_acl_safely --operation Describe --topic topic-1
add_acl_safely --operation Describe --topic topic-2
add_acl_safely --operation Read --topic __consumer_offsets
add_acl_safely --operation Write --topic __consumer_offsets
add_acl_safely --operation Describe --group "$GROUP_ID"
add_acl_safely --operation Read     --group "$GROUP_ID"

log "Done. Topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --list || true

log "ACLs (all):"
"$KAFKA_ACLS" --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$USED_CONFIG" --list 2>/dev/null || log "No ACLs found"

exit 0