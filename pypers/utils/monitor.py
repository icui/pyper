from pypers import Directory, hasarg, basedir as d


if __name__ == '__main__':
    if hasarg('m'):
        # monitor job status
        from curses import wrapper
        from time import sleep
        from threading import Thread

        def main(stdscr):
            stdscr.scrollok(1)
            ended = False

            def loop():
                n = 0
                while not ended:
                    if n % 50 == 0:
                        stdscr.clear()
                        stdscr.addstr(d.load('job.pickle').info + '\n')
                        stdscr.refresh()

                    sleep(0.1)
                    n += 1
            
            thread = Thread(target=loop)
            thread.start()

            while True:
                c = stdscr.getch()
                if c == ord('q') or c == ord(' '):
                    break

            ended = True
        
        wrapper(main)
    
    else:
        # print job status
        print(d.load('job.pickle').info)
