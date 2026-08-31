import json
import random
import time

from confluent_kafka import Producer

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'scaling-demo'
PAGES = ['home', 'about', 'pricing', 'contact', 'blog']
USERS = ['alice', 'bob', 'carol', 'dave', 'eve', 'frank', 'grace', 'henry', 'jean', 'louis']


def main() -> None:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print(f'\n[PRODUCER] Writing to "{TOPIC}" every 0.5s. Press Ctrl+C to stop.')
    print('[PRODUCER] Start scaling_consumers_worker.py --worker-id 1 (then 2, then 3)\n')
    print(f'  {"SEQ":>6}  {"PARTITION":>9}  {"OFFSET":>6}  {"USER":<8}  PAGE')
    print('  ' + '-' * 44)

    seq = 0
    try:
        while True:
            user = random.choice(USERS)
            event = {'seq': seq, 'user': user, 'page': random.choice(PAGES)}

            def on_delivery(err, msg, seq=seq, user=user):
                if err:
                    print(f'  [ERROR] {err}')
                else:
                    print(
                        f'  {seq:>6}  {msg.partition():>9}  {msg.offset():>6}'
                        f'  {user:<8}  {json.loads(msg.value())["page"]}'
                    )

            producer.produce(TOPIC, key=user, value=json.dumps(event), callback=on_delivery)
            producer.poll(0)
            seq += 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        producer.flush()
        print(f'\n[PRODUCER] Stopped after {seq} messages.')


if __name__ == '__main__':
    main()
