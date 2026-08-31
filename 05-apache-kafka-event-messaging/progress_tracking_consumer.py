import argparse
import json
from confluent_kafka import Consumer, TopicPartition

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'progress-tracking-demo'
GROUP_ID = 'progress-tracking-group'
NUM_PARTITIONS = 3


def format_committed(consumer: Consumer) -> str:
    """Return a human-readable snapshot of committed offsets for all partitions."""
    tps = [TopicPartition(TOPIC, p) for p in range(NUM_PARTITIONS)]
    committed = consumer.committed(tps, timeout=5)
    parts = []
    for tp in committed:
        offset = tp.offset if tp.offset >= 0 else 'none'
        parts.append(f'p{tp.partition}={offset}')
    return '  '.join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--fail-after', type=int, default=0,
        help='Simulate a crash after N successfully committed messages (0 = no crash)',
    )
    args = parser.parse_args()

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # manual commit so we can show each step
    })
    consumer.subscribe([TOPIC])

    print(f'[INFO] Consumer group : {GROUP_ID}')
    print(f'[INFO] Commit mode    : manual (enable.auto.commit=false)')
    if args.fail_after:
        print(f'[INFO] Crash after    : {args.fail_after} messages\n')
    else:
        print('[INFO] Crash after    : never (normal run)\n')

    # Show offsets BEFORE we start reading so it's clear where we resume from
    # (need a brief poll to trigger assignment first)
    consumer.poll(2.0)
    print(f'[OFFSETS IN __consumer_offsets] {format_committed(consumer)}')
    print()

    count = 0
    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                if count > 0:
                    print('\n[INFO] No more messages — all events processed.')
                continue
            if msg.error():
                print(f'[ERROR] {msg.error()}')
                continue

            count += 1
            event = json.loads(msg.value())
            print(
                f'[PROCESSING #{count:<3}]  partition={msg.partition()}'
                f'  offset={msg.offset():<3}  seq={event["seq"]}'
            )

            # Commit this single message's offset synchronously
            consumer.commit(message=msg, asynchronous=False)

            # Show the updated state of __consumer_offsets after the commit
            print(f'  [OFFSETS IN __consumer_offsets] {format_committed(consumer)}')

            if args.fail_after and count >= args.fail_after:
                raise RuntimeError(
                    f'Simulated crash after {args.fail_after} committed messages!'
                )

    except RuntimeError as exc:
        print(f'\n[CRASH] {exc}')
        print(f'[CRASH] Committed offsets are safely stored in __consumer_offsets.')
        print(f'[CRASH] Re-run without --fail-after to see recovery:')
        print(f'          uv run progress_tracking_consumer.py')

    except KeyboardInterrupt:
        print(f'\n[INFO] Stopped by user after {count} messages.')

    finally:
        consumer.close()

    print(f'\n[KEY INSIGHT]')
    print('🧐 Committed offsets live in the internal __consumer_offsets topic.')
    print('🧐 On restart the consumer fetches those offsets and resumes from there.')
    print('🧐 Only messages with offsets >= committed offset are replayed after a crash.')
    print('🧐 With manual commit you control exactly which messages are "safe".')


if __name__ == '__main__':
    main()
