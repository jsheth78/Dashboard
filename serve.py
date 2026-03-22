"""Serve the dashboard locally. Open http://localhost:8000 in your browser."""

import http.server
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
print("Serving dashboard at http://localhost:8000  (Ctrl+C to stop)")
http.server.HTTPServer(("", 8000), http.server.SimpleHTTPRequestHandler).serve_forever()
