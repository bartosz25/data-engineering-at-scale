import argparse
import json
import time
from confluent_kafka import Consumer

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'queues-demo'
GROUP_ID = 'jobs-regular-group'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--worker-id', required=True, type=int,
        help='Numeric worker identifier (1, 2, ...)',
    )
    args = parser.parse_args()
    label = f'[WORKER-{args.worker_id}]'

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })

    assigned_partitions: list[int] = []

    def on_assign(_, partitions):
        assigned_partitions.clear()
        assigned_partitions.extend(p.partition for p in partitions)
        if assigned_partitions:
            print(f'{label} Rebalance — assigned partitions: {assigned_partitions}')
        else:
            print(
                f'{label} Rebalance — assigned NO partitions. '
                f'This worker is IDLE (topic has only 1 partition, already owned by another worker).'
            )

    def on_revoke(_, partitions):
        if partitions:
            print(f'{label} Rebalance — revoking partitions: {[p.partition for p in partitions]}')

    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    print(f'{label} Regular consumer group worker started.')
    print(f'{label} Group ID     : {GROUP_ID}')
    print(f'{label} Topic        : {TOPIC}  (1 partition)')
    print(f'{label} With a regular consumer group only ONE worker can own the single')
    print(f'{label} partition — the second worker is assigned nothing and never sees data.')
    print(f'{label} Compare with queues_consumer.py (ShareConsumer) where BOTH workers')
    print(f'{label} receive messages from the same partition.\n')
    print(f'  {label}  {"JOB_ID":>7}  {"TYPE":<22}  {"PARTITION":>9}  {"OFFSET":>6}')
    print('  ' + '-' * 68)

    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                if not assigned_partitions:
                    print(f'{label} ... waiting (no partitions assigned — this worker is idle)')
                continue
            if msg.error():
                print(f'{label} [ERROR] {msg.error()}')
                continue

            job = json.loads(msg.value())
            print(
                f'  {label}  {job["job_id"]:>7}  {job["type"]:<22}'
                f'  {msg.partition():>9}  {msg.offset():>6}'
            )

            time.sleep(0.3)

            # Regular commit — advances the offset for the whole group
            consumer.commit(asynchronous=False)
            processed += 1

    except KeyboardInterrupt:
        print(f'\n{label} Stopped. Processed {processed} jobs.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()