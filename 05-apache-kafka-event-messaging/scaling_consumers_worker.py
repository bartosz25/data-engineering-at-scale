import argparse
import json

from colorama import Back, Style
from confluent_kafka import Consumer

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'scaling-demo'
GROUP_ID = 'scaling-consumer-group'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--worker-id', required=True, type=int,
        help='Numeric worker identifier (1, 2, 3, ...)',
    )
    args = parser.parse_args()
    label = f'[WORKER-{args.worker_id}]'

    def on_assign(consumer, partitions):
        ids = sorted(p.partition for p in partitions)
        print(f'\n{Back.WHITE}{label} *** REBALANCE: assigned partitions {ids} ***{Style.RESET_ALL}\n')

    def on_revoke(consumer, partitions):
        ids = sorted(p.partition for p in partitions)
        print(f'\n{label} *** REBALANCE: revoking partitions {ids} ***\n')

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',  # only new messages after the worker joins
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    print(f'{label} Started. Joined consumer group "{GROUP_ID}".')
    print(f'{label} Waiting for partition assignment...')

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'{label} [ERROR] {msg.error()}')
                continue

            event = json.loads(msg.value())
            print(
                f'{label}  partition={msg.partition()}  offset={msg.offset():<4}'
                f'  seq={event["seq"]:<4}  user={event["user"]:<6}  page={event["page"]}'
            )
    except KeyboardInterrupt:
        print(f'\n{label} Stopped.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
