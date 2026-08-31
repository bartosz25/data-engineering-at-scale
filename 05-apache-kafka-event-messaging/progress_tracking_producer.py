import json
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'progress-tracking-demo'
NUM_MESSAGES = 50


def main() -> None:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print(f'\n[PRODUCER] Writing {NUM_MESSAGES} events to "{TOPIC}"...\n')
    print(f'  {"SEQ":>4}  {"PARTITION":>9}  {"OFFSET":>6}')
    print('  ' + '-' * 24)

    for i in range(NUM_MESSAGES):
        event = {'seq': i, 'payload': f'event-{i:04d}'}

        def on_delivery(err, msg, i=i):
            if err:
                print(f'  [ERROR] seq {i}: {err}')
            else:
                print(f'  {i:>4}  {msg.partition():>9}  {msg.offset():>6}')

        # Use seq % 3 as key so events are distributed evenly across all 3 partitions
        producer.produce(TOPIC, key=str(i % 3), value=json.dumps(event), callback=on_delivery)
        producer.poll(0)

    producer.flush()
    print(f'\n[DONE] {NUM_MESSAGES} events written.')


if __name__ == '__main__':
    main()
