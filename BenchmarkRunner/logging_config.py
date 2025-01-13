import logging

def setup_logging():
    logging.basicConfig(
        format="{levelname}: {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO
    )
