import json
import random
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from shared_config import BOOTSTRAP_SERVERS

TOPIC = 'queues-demo'
JOB_TYPES = [
    'resize-image',
    'send-email',
    'generate-report',
    'sync-data',
    'cleanup-cache',
    'index-document',
]


def main() -> None:
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print(f'\n[PRODUCER] Enqueuing jobs to "{TOPIC}" every second. Press Ctrl+C to stop.')
    print('[PRODUCER] Start queues_consumer.py --worker-id 1 (then 2, then 3) to process jobs.\n')
    print(f'  {"JOB_ID":>7}  {"PARTITION":>9}  {"OFFSET":>6}  TYPE')
    print('  ' + '-' * 44)

    job_id = 0
    try:
        while True:
            job_type = random.choice(JOB_TYPES)
            job = {'job_id': job_id, 'type': job_type, 'ts': time.time()}

            def on_delivery(err, msg, job_id=job_id, job_type=job_type):
                if err:
                    print(f'  [ERROR] job {job_id}: {err}')
                else:
                    print(
                        f'  {job_id:>7}  {msg.partition():>9}  {msg.offset():>6}'
                        f'  {job_type}'
                    )

            producer.produce(TOPIC, value=json.dumps(job), callback=on_delivery)
            producer.poll(0)
            job_id += 1
            time.sleep(1)
    except KeyboardInterrupt:
        producer.flush()
        print(f'\n[PRODUCER] Stopped after {job_id} jobs.')


if __name__ == '__main__':
    main()
