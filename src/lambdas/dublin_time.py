"""Europe/Dublin time zone. Uses the system time-zone database when it exists; some minimal Lambda
images ship without it, so fall back to the EU summer-time rule (last Sunday of March to last Sunday
of October, 01:00 UTC)."""
from datetime import datetime, timedelta, tzinfo

try:
    from zoneinfo import ZoneInfo
    DUBLIN = ZoneInfo("Europe/Dublin")
except Exception:                                   # ZoneInfoNotFoundError on images without tzdata
    def _last_sunday(year, month):
        d = datetime(year, month + 1, 1) - timedelta(days=1)
        return d - timedelta(days=(d.weekday() - 6) % 7)

    class _Dublin(tzinfo):
        def _summer(self, dt):
            naive = dt.replace(tzinfo=None)
            start = _last_sunday(naive.year, 3).replace(hour=1)
            end = _last_sunday(naive.year, 10).replace(hour=1)
            return start <= naive < end

        def utcoffset(self, dt):
            return timedelta(hours=1) if self._summer(dt) else timedelta(0)

        def dst(self, dt):
            return self.utcoffset(dt)

        def tzname(self, dt):
            return "IST" if self._summer(dt) else "GMT"

    DUBLIN = _Dublin()
