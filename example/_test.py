#!/usr/bin/env python

from time import sleep

def test(arg: int):
    print(f'running task {arg}')

    sleep(3)

    print(f'task {arg} done')
