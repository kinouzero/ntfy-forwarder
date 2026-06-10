import asyncio

shutdown_event = asyncio.Event()

telegram_queue = asyncio.Queue()

workers = {}
worker_last_seen = {}

aggregation_buffer = {}
digest_buffer = {}
recent_events = {}
topic_stats = {}
topic_rates = {}
