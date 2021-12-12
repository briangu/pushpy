import time


def main(main_control):
    while main_control.thread_continue:
        print(f"hello")
        time.sleep(1)
