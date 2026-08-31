"""
Multi-Reader Demo — Consumer B  (group.id = reader-group-b)
===========================================================
Reads every event from the "multi-reader-demo" topic from the very beginning.

Run alongside multi_reader_consumer_a.py to confirm that two consumers
with DIFFERENT group IDs each receive the full stream independently — Kafka
does not remove a message just because one consumer has read it.

Run (in its own terminal, after multi_reader_producer.py):
    uv run multi_reader_consumer_b.py
"""

import json
from confluent_kafka import Consumer

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'multi-reader-demo'
GROUP_ID = 'reader-group-b'
LABEL = '[GROUP-B]'


def main() -> None:
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([TOPIC])

    print(f'{LABEL} Consumer started  group.id={GROUP_ID}')
    print(f'{LABEL} Reading "{TOPIC}" from the beginning — will receive ALL events.\n')
    print(f'  {LABEL}  {"PARTITION":>9}  {"OFFSET":>6}  {"EVENT_ID":>8}  {"USER":<8}  PAGE')
    print('  ' + '-' * 56)

    received = 0
    idle_polls = 0
    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                idle_polls += 1
                if received > 0 and idle_polls >= 3:
                    print(f'\n{LABEL} No more messages. Total received: {received}')
                    print(f'{LABEL} Committed offsets are stored under group.id="{GROUP_ID}".')
                    print(f'{LABEL} Consumer A has its own independent offsets.')
                    break
                continue
            if msg.error():
                print(f'{LABEL} [ERROR] {msg.error()}')
                continue

            idle_polls = 0
            received += 1
            event = json.loads(msg.value())
            print(
                f'  {LABEL}  {msg.partition():>9}  {msg.offset():>6}'
                f'  {event["event_id"]:>8}  {event["user"]:<8}  {event["page"]}'
            )
    except KeyboardInterrupt:
        print(f'\n{LABEL} Stopped. Received {received} events.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
