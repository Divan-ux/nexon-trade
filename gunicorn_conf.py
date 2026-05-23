import os
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
worker_class = "eventlet"   # ← asynchronous worker
workers = 1
timeout = 120
worker_hooks = ["server:app"]
