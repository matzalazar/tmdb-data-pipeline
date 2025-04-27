import logging
from pathlib import Path

def setup_logger(script_name):
    Path('logs').mkdir(exist_ok=True)
    log_file = f'logs/{script_name}.log'
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    return logging.getLogger(script_name)
