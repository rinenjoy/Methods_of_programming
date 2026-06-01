import time
import module


if __name__ == "__main__":
    module.main()
    # Главный поток держим живым, чтобы фоновые daemon-потоки
    # (web/consumer/producer) не умирали вместе с ним.
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass

