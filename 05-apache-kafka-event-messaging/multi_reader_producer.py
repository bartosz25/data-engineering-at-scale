import json
import random
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'multi-reader-demo'
NUM_MESSAGES = 30
PAGES = ['home', 'about', 'pricing', 'contact', 'blog']
USERS = ['alice', 'bob', 'carol', 'dave', 'eve', 'frank', 'grace', 'henry']


def main() -> None:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print(f'\n[PRODUCER] Writing {NUM_MESSAGES} events to "{TOPIC}"...\n')
    print(f'  {"EVENT_ID":>8}  {"PARTITION":>9}  {"OFFSET":>6}  {"USER":<8}  PAGE')
    print('  ' + '-' * 48)

    for i in range(NUM_MESSAGES):
        user = random.choice(USERS)
        page = random.choice(PAGES)
        event = {'event_id': i, 'user': user, 'page': page, 'ts': time.time()}

        def on_delivery(err, msg, i=i, user=user, page=page):
            if err:
                print(f'  [ERROR] event {i}: {err}')
            else:
                print(
                    f'  {i:>8}  {msg.partition():>9}  {msg.offset():>6}'
                    f'  {user:<8}  {page}'
                )

        producer.produce(TOPIC, key=user, value=json.dumps(event), callback=on_delivery)
        producer.poll(0)

    producer.flush()
    print(f'\n[DONE] {NUM_MESSAGES} events written to "{TOPIC}".')
    print('\n[NEXT] Open two terminals and run:')
    print('  terminal 1: uv run multi_reader_consumer_a.py')
    print('  terminal 2: uv run multi_reader_consumer_b.py')
    print('\nBoth consumers will receive ALL 30 events independently.')


if __name__ == '__main__':
    main()
