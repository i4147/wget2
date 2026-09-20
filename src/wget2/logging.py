from sys import stdout

from loguru import logger

log_handler_id: int | None = None


def set_logger_level(level: str | int):
    global log_handler_id
    logger.remove(log_handler_id)
    log_handler_id = logger.add(sink=stdout, level=level)


def main():
    logger.remove()
    log_handler_id = logger.add(sink=stdout)


if __name__ == "__main__":
    main()
