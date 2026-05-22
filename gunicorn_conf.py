import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
worker_class = "eventlet"
workers = 1
timeout = 120
