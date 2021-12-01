import time

from threading import Thread
from java.util.concurrent.atomic import AtomicInteger, AtomicBoolean

# Essentially we want to know during each minute period how many requests were successful


class AbstractTimedThroughputWorker(object):
    """ Send throughput across a time period. """

    def __init__(self, period=60, chunks=100, throughput=0):
        self.period = period  # The time period over which to produce throughput
        self.chunks = chunks
        self.throughput = AtomicInteger(throughput)
        self.throughput_success = AtomicInteger(0)
        self.resume = AtomicBoolean(True)
        self.curr_tick = AtomicInteger(0)

    def stop(self):
        self.resume = self.resume.set(False)
        self.thread.join()

    def start(self):
        self.thread = Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        while self.resume:
            self.throughput_success.set(self.tick(self.throughput.get()))

    def action(self, throughput):
        """ This method fires

        Args:
            throughput (int): The throughput in bytes

        Returns (bool): Indicating success.
        """
        raise NotImplementedError("Please implement this method")

    def next_tick(self, period):
        """ Returns the next multiple of a time period """
        curr_time = time.time()
        return curr_time + (period - curr_time % period)

    def tick(self, throughput):
        """ Fires throughput over this time period """
        # The next 60 second time period
        next_tick = self.next_tick(self.period)

        # Every mini_tick, we will fire a thoughput of this size
        throughput_per_chunk = throughput / self.chunks

        #  The size of a mini_tick (e.g. 0.6 seconds)
        mini_tick_period = self.period / float(self.chunks)

        chunks_sent, successes = 0, 0
        while time.time() < next_tick and chunks_sent < self.chunks:
            # Fire action and record time taken
            # time_taken, success = time_it(self.action, throughput_per_chunk)
            success = self.action(throughput_per_chunk)

            # Count successes
            if success:
                successes += 1

            chunks_sent += 1

            # The time remaining to reach the next mini tick
            time_till_next_mini_tick = max(
                0, self.next_tick(mini_tick_period) - time.time())

            # sleep to next mini tick to ensure actions happen evenly
            time.sleep(time_till_next_mini_tick)

        return successes * throughput_per_chunk
