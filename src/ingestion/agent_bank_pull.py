"""
MarginWatch - Ingestion: pull the agent bank's daily reports over its REST API and land them in S3.

Flow (one run = one business date):
  1. get an OAuth2 access token (client-credentials grant); re-use it until it stops working
  2. LIST the day's reports, following the cursor until next_cursor is null (pagination)
  3. for each report not already landed: DOWNLOAD it, VERIFY its SHA-256, LAND it in S3
  4. record it in the state store (DynamoDB in AWS, a JSON file locally) so re-runs skip it

Production behaviours built in:
  - timeouts on every call (a hung connection must not hang the job)
  - retry ONLY what is retryable: 429 (honour Retry-After), 5xx and network errors with
    exponential backoff + jitter; 401 -> refresh token once; other 4xx -> fail fast
  - bounded retries, then a clear error (the orchestrator decides what happens next)
  - secrets come from the environment here; in AWS they come from Secrets Manager
"""
import base64, hashlib, json, os, random, tempfile, time
import urllib.error, urllib.parse, urllib.request

from upload_to_landing import land_file

RETRYABLE = {429, 500, 502, 503, 504}


class BankApiError(Exception):
    pass


class BankApiClient:
    def __init__(self, base_url, client_id, client_secret, timeout=10, max_attempts=5, log=print):
        self.base, self.cid, self.secret = base_url.rstrip("/"), client_id, client_secret
        self.timeout, self.max_attempts, self.log = timeout, max_attempts, log
        self._token = None

    # ---- auth -------------------------------------------------------------------------------
    def _fetch_token(self):
        basic = base64.b64encode(f"{self.cid}:{self.secret}".encode()).decode()
        req = urllib.request.Request(f"{self.base}/oauth2/token", method="POST",
                                     data=urllib.parse.urlencode({"grant_type": "client_credentials",
                                                                  "scope": "reports.read"}).encode(),
                                     headers={"Authorization": f"Basic {basic}",
                                              "Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                self._token = json.load(r)["access_token"]
        except urllib.error.HTTPError as e:     # bad/rotated credentials: alert a human, don't retry
            raise BankApiError(f"token request rejected ({e.code}) - check client secret in Secrets Manager") from None
        self.log("  auth   : new access token (client_credentials)")

    # ---- one HTTP call with the retry policy ------------------------------------------------
    def _get(self, path, params=None):
        url = f"{self.base}{path}" + (f"?{urllib.parse.urlencode(params)}" if params else "")
        refreshed = False
        for attempt in range(1, self.max_attempts + 1):
            if self._token is None:
                self._fetch_token()
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._token}",
                                                       "Accept": "application/json, text/csv"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return r.read(), dict(r.headers)
            except urllib.error.HTTPError as e:
                code = e.code
                if code == 401 and not refreshed:            # token expired/revoked: refresh ONCE
                    self.log(f"  {code}    : token rejected -> refreshing")
                    self._token, refreshed = None, True
                    continue
                if code not in RETRYABLE:                    # 400/403/404 etc. retrying will not help
                    raise BankApiError(f"{code} on {path}: {e.read()[:200]!r}") from None
                wait = float(e.headers.get("Retry-After") or (0.5 * 2 ** (attempt - 1)))
            except (urllib.error.URLError, TimeoutError) as e:  # DNS, refused, reset, timeout
                code, wait = type(e).__name__, 0.5 * 2 ** (attempt - 1)
            wait += random.uniform(0, 0.25)                      # jitter: avoid retry stampedes
            self.log(f"  {code}    : attempt {attempt}/{self.max_attempts}, retrying in {wait:.2f}s")
            time.sleep(wait)
        raise BankApiError(f"gave up on {path} after {self.max_attempts} attempts")

    # ---- endpoints --------------------------------------------------------------------------
    def list_reports(self, business_date, page_size=50):
        cursor, page = None, 0
        while True:
            params = {"business_date": business_date, "page_size": page_size}
            if cursor:
                params["cursor"] = cursor
            body, _ = self._get("/v1/reports", params)
            doc = json.loads(body)
            page += 1
            self.log(f"  list   : page {page} -> {len(doc['data'])} report(s), next_cursor={doc['next_cursor']}")
            yield from doc["data"]
            cursor = doc["next_cursor"]
            if not cursor:
                return

    def download(self, report):
        body, headers = self._get(f"/v1/reports/{report['report_id']}/content")
        digest = hashlib.sha256(body).hexdigest()
        expected = {report["sha256"], headers.get("X-Content-SHA256", report["sha256"])}
        if expected != {digest} or len(body) != report["size_bytes"]:
            raise BankApiError(f"integrity check failed for {report['report_id']} (truncated or corrupted)")
        return body, digest


def load_state(path):
    return json.load(open(path)) if os.path.exists(path) else {}


def save_state(path, state):
    tmp = path + ".tmp"
    json.dump(state, open(tmp, "w"), indent=1)
    os.replace(tmp, path)                     # atomic: never leave a half-written state file


def pull_agent_bank(client, s3, bucket, business_date, state_path, log=print):
    state, summary = load_state(state_path), {"landed": 0, "skipped_already_landed": 0}
    for rep in client.list_reports(business_date, page_size=1):     # page_size=1 just to show paging
        done = state.get(rep["report_id"])
        if done and done["sha256"] == rep["sha256"]:
            log(f"  skip   : {rep['report_id']} {rep['file_name']} already landed (same sha256)")
            summary["skipped_already_landed"] += 1
            continue
        body, digest = client.download(rep)
        log(f"  verify : {rep['file_name']} {len(body):,} bytes sha256={digest[:12]} OK")
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, rep["file_name"])
            open(local, "wb").write(body)
            status, _ = land_file(s3, bucket, {
                "local_path": local,
                "key": f"source=agent_bank/business_date={business_date}/{rep['file_name']}",
                "metadata": {"source-system": "agent_bank", "business-date": business_date,
                             "received-at-utc": rep["created_at"], "message-id": rep["report_id"],
                             "sender": "agent-bank-api"}})
        log(f"  land   : {status} source=agent_bank/business_date={business_date}/{rep['file_name']}")
        state[rep["report_id"]] = {"sha256": digest, "file_name": rep["file_name"], "status": status}
        save_state(state_path, state)          # checkpoint after EVERY file, so a crash loses nothing
        summary["landed"] += 1
    return summary
