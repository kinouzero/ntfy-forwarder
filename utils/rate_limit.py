import time
from collections import deque


class RateLimiter:
    def __init__(self, max_events, window_seconds):
        self.max_events = max_events
        self.window_seconds = window_seconds
        self.events = deque()

    def allow(self, now=None):
        if self.max_events <= 0:
            return True

        if now is None:
            now = time.time()

        cutoff = now - self.window_seconds
        while self.events and self.events[0] <= cutoff:
            self.events.popleft()

        if len(self.events) >= self.max_events:
            return False

        self.events.append(now)
        return True
