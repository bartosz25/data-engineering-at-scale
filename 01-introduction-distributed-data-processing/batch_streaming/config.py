def _get_base_dir() -> str:
    return '/tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming'

def get_demo_dir_for_streaming() -> str:
    return f'{_get_base_dir()}/streaming'

def get_demo_dir_for_batch() -> str:
    return f'{_get_base_dir()}/batch'
