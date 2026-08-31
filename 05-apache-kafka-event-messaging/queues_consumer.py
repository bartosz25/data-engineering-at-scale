import argparse
import json
import time
from confluent_kafka import ShareConsumer, AcknowledgeType

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'queues-demo'
SHARE_GROUP = 'jobs-queue23'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--worker-id', required=True, type=int,
        help='Numeric worker identifier (1, 2, 3, ...)',
    )
    args = parser.parse_args()
    label = f'[WORKER-{args.worker_id}]'

    consumer = ShareConsumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': SHARE_GROUP,
        'share.acknowledgement.mode': 'explicit',
    })
    consumer.subscribe([TOPIC])

    print(f'{label} Share group worker started.')
    print(f'{label} Share group  : {SHARE_GROUP}')
    print(f'{label} Topic        : {TOPIC}')
    print(f'{label} Each job_id should appear in exactly ONE worker — queue semantics.')
    print(f'{label} Using acknowledge() instead of commit().\n')
    print(f'  {label}  {"JOB_ID":>7}  {"TYPE":<22}  {"PARTITION":>9}  {"OFFSET":>6}')
    print('  ' + '-' * 68)

    processed = 0
    try:
        while True:
            records = consumer.poll(timeout=5.0)
            for record in records.records():
                job = json.loads(record.value())
                print(
                    f'  {label}  {job["job_id"]:>7}  {job["type"]:<22}'
                    f'  {record.partition():>9}  {record.offset():>6}'
                )

                # Simulate processing work (e.g. resizing an image, sending an email)
                time.sleep(0.3)

                # Acknowledge = "this job is done, do not redeliver it"
                # Unlike commit(), acknowledgement is per-message, not per-offset.
                consumer.acknowledge(record, ack_type=AcknowledgeType.ACCEPT)
                processed += 1

    except KeyboardInterrupt:
        print(f'\n{label} Stopped. Processed {processed} jobs.')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
