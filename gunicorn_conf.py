# Gunicorn configuration file
# This tells Gunicorn to use the post_worker_init hook defined in our app.

import os

# Bind to the port Render provides
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

# Worker settings
worker_class = "eventlet"
workers = 1
timeout = 120

# Crucial: tells Gunicorn to call app.post_worker_init after worker starts
worker_hooks = ["server:app"]