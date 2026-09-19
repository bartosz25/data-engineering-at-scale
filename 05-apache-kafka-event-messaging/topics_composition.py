import dataclasses
import json
import time

from colorama import Style
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'topics-demo'

@dataclasses.dataclass(frozen=True)
class Message:
    key: str
    body: str

def send_records(with_duplicated_keys: bool, with_unbalanced_keys: bool) -> None:
    print('Produced records and their stats...')
    print('-' * 42)

    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    def on_delivery(err, msg):
        if err:
            print(f'[ERROR] delivery failed: {err}')
        else:
            key = msg.key().decode()
            value = msg.value().decode()
            print(f'Received callback for {key}={value}')
            print(f'{"KEY":<8}  {"PAGE":<10}  {"PARTITION":>9}  {"OFFSET":>6}')
            val = json.loads(value)
            print(f'{key:<8}  {val:<10}  {msg.partition():>9}  {msg.offset():>6}')

    records = [
        Message(key='France', body='Paris'),
        Message(key='England', body='London'),
        Message(key='Poland', body='Warsaw'),
        Message(key='Germany', body='Berlin'),
        Message(key='France', body='Lyon'),
        Message(key='England', body='Manchester'),
        Message(key='Poland', body='Poznan'),
        Message(key='Germany', body='Munich'),
    ]
    if with_duplicated_keys:
        records.append(Message(key='France', body='Rennes'))
        records.append(Message(key='France', body='Rennes'))
    if with_unbalanced_keys:
        records.append(Message(key='France', body='Lille'))
    for m in records:
        producer.produce(TOPIC, key=m.key, value=json.dumps(m.body), callback=on_delivery)

    print('Since the producer is asynchronous, we force delivering all the messages that might be pending in memory...')
    producer.flush()
    print('🧐 Cities for the same country always land on the same partition — key hashing is deterministic.')
    print('🧐 Offsets within each partition are independent and monotonically increasing.')


def consume() -> None:
    print('\n--- Reading the producer records ---')

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'topics-composition-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([TOPIC])

    # The demo topic has 2 partitions
    partition_counts = {0: 0, 1: 0}
    received = 0
    # The first poll really assigns a consumer to the partitions; subscribe is just
    # a marker for the consumer to join the group (a bit like lazy evaluation in Apache Spark)
    msg = None
    first_run = True
    while msg or first_run:
        msg = consumer.poll(5.0)
        if msg is None:
            continue
        if msg.error():
            print(f'[ERROR] {msg.error()}')
            continue

        val = json.loads(msg.value())
        partition_counts[msg.partition()] += 1
        print(f'{"PARTITION":>9}  {"OFFSET":>6}  {"KEY":<8}  {"PAGE"}  {"TIMESTAMP"}')
        print('-' * 56)
        _, ts_ms = msg.timestamp()
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts_ms / 1000))
        print(f'{msg.partition():>9}  {msg.offset():>6} {msg.key().decode():<8}  {val:<6}  {ts}')
        received += 1
        # Commit means we have successfully processed the message and don't want to reprocess it
        # if the consumer crashes or is restarted
        consumer.commit(asynchronous=False)
        first_run = False

    consumer.close()

    print('\n[SUMMARY] Messages per partition:')
    for p, count in partition_counts.items():
        bar = '█' * count
        print(f'  partition {p}: {count:>2} messages  {bar}')

    print('\n[KEY INSIGHT]')
    print('🧐 Each record has a unique address: (topic, partition, offset).')
    print('🧐 Offsets are per-partition — partition 0 has its own sequence, partition 1 has its own.')
    print('🧐 The same message key always routes to the same partition (murmur2 hash % num_partitions).')
    print('🧐 Ordering is guaranteed within a partition, NOT across partitions.')


if __name__ == '__main__':
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print("Let's see the existing topics. The topic we use in this demo should have already been created")
    existing = admin.list_topics(timeout=5).topics
    for existing_topic in existing:
        print(f'Got existing topic={Style.BRIGHT}{existing_topic}{Style.RESET_ALL}')


    send_records(
        with_duplicated_keys=False, with_unbalanced_keys=False
    )

    response_1 = input('Ready to move to the consumer?').strip().lower()

    consume()
    print('-------------------------------------------------------------------------')
    print('-------------------------------------------------------------------------')
    print('Sending one additional record for France, showing unbalanced partition')
    response_2 = input('Ready to move to the consumer and see the unbalanced partitions?').strip().lower()
    send_records(
        with_duplicated_keys=False, with_unbalanced_keys=True
    )
    consume()
    print('-------------------------------------------------------------------------')
    print('-------------------------------------------------------------------------')
    print('Sending two additional records for France, showing duplicated keys')
    response_3 = input('Ready to move to the consumer and see the duplicated keys?').strip().lower()
    send_records(
        with_duplicated_keys=True, with_unbalanced_keys=False
    )
    consume()

    print('Done!')