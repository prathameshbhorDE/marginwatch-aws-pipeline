"""
A small fake 'agent bank' REST API, so the ingestion client can be built and tested locally.
It behaves like real bank APIs do on a bad day:
  - OAuth2 client-credentials token that expires after a few calls (forces a refresh)
  - cursor pagination on the report list
  - one 429 Too Many Requests (with Retry-After) and one 503 Service Unavailable
  - SHA-256 of each file in the listing AND in a response header, so the client can verify it
"""
import base64, hashlib, json, os, threading, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

CLIENT_ID, CLIENT_SECRET = "orion-collateral-ops", "s3cr3t-from-secrets-manager"
TOKEN_CALL_LIMIT = 3            # a token stops working after 3 API calls (simulates expiry)

class Bank:
    def __init__(self, files_dir, business_date):
        self.reports, self.tokens, self.lock = [], {}, threading.Lock()
        self.fail_once = {"list_429": True, "download_503": True}
        for i, (rtype, name) in enumerate([("TRADES_MTM", "trades_mtm"), ("SECURITIES_HOLDINGS", "securities_holdings")]):
            fname = f"{name}_{business_date.replace('-', '')}.csv"
            body = open(os.path.join(files_dir, fname), "rb").read()
            self.reports.append({"report_id": f"RPT-{business_date}-{i + 1:03d}", "report_type": rtype,
                                 "business_date": business_date, "file_name": fname, "content_type": "text/csv",
                                 "size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                                 "created_at": f"{business_date}T23:40:0{i}Z", "_body": body})

def make_handler(bank):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # keep demo output clean
            pass

        def _send(self, code, payload=None, headers=None, raw=None):
            body = raw if raw is not None else json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json" if raw is None else "text/csv")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _auth_ok(self):
            tok = self.headers.get("Authorization", "").removeprefix("Bearer ")
            with bank.lock:
                if tok not in bank.tokens or bank.tokens[tok] >= TOKEN_CALL_LIMIT:
                    return False
                bank.tokens[tok] += 1
            return True

        def do_POST(self):
            if self.path != "/oauth2/token":
                return self._send(404, {"error": "not_found"})
            basic = base64.b64decode(self.headers.get("Authorization", "Basic ").split()[1] or b"").decode()
            if basic != f"{CLIENT_ID}:{CLIENT_SECRET}":
                return self._send(401, {"error": "invalid_client"})
            tok = uuid.uuid4().hex
            bank.tokens[tok] = 0
            self._send(200, {"access_token": tok, "token_type": "Bearer", "expires_in": 300, "scope": "reports.read"})

        def do_GET(self):
            url = urlparse(self.path)
            if not self._auth_ok():
                return self._send(401, {"error": "invalid_token"})
            if url.path == "/v1/reports":
                if bank.fail_once["list_429"]:
                    bank.fail_once["list_429"] = False
                    return self._send(429, {"error": "rate_limited"}, {"Retry-After": "1"})
                q = parse_qs(url.query)
                rows = [r for r in bank.reports if r["business_date"] == q["business_date"][0]]
                start, size = int(q.get("cursor", ["0"])[0]), int(q.get("page_size", ["50"])[0])
                page = [{k: v for k, v in r.items() if k != "_body"} for r in rows[start:start + size]]
                nxt = str(start + size) if start + size < len(rows) else None
                return self._send(200, {"data": page, "next_cursor": nxt})
            if url.path.startswith("/v1/reports/") and url.path.endswith("/content"):
                rid = url.path.split("/")[3]
                rep = next((r for r in bank.reports if r["report_id"] == rid), None)
                if rep is None:
                    return self._send(404, {"error": "report_not_found"})
                if rep["report_type"] == "SECURITIES_HOLDINGS" and bank.fail_once["download_503"]:
                    bank.fail_once["download_503"] = False
                    return self._send(503, {"error": "service_unavailable"})
                return self._send(200, raw=rep["_body"], headers={"X-Content-SHA256": rep["sha256"]})
            self._send(404, {"error": "not_found"})
    return H

def start(files_dir, business_date, port=0):
    bank = Bank(files_dir, business_date)
    srv = ThreadingHTTPServer(("127.0.0.1", port), make_handler(bank))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"
