"""Pure time-mapping utilities for simulated event replay (Task 2.5).

Maps historical CSV timestamps into:
1. Simulated event time (UTC): sim_time = SIM_START + (orig_time - orig_t0)
2. Real wall-clock dispatch deadline: target_wall = t_start + (orig_time - orig_t0) / speed_factor
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

UTC = timezone.utc


def ensure_utc(dt: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC (treats naive datetimes as UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_sim_start(sim_start_date: str) -> datetime:
    """Parse SIM_START_DATE ('YYYY-MM-DD' or ISO-8601 UTC string) into UTC datetime."""
    cleaned = sim_start_date.strip()
    if len(cleaned) == 10 and cleaned[4] == "-" and cleaned[7] == "-":
        return datetime.strptime(cleaned, "%Y-%m-%d").replace(tzinfo=UTC)
    return ensure_utc(datetime.fromisoformat(cleaned.replace("Z", "+00:00")))


@dataclass(frozen=True)
class TimeMapper:
    """Deterministic time compression mapper from original CSV timestamps to simulation time."""

    orig_t0: datetime
    sim_start: datetime
    t_start_monotonic: float
    speed_factor: float = 60.0

    def __post_init__(self) -> None:
        if self.speed_factor <= 0:
            raise ValueError(f"speed_factor must be > 0, got {self.speed_factor}")
        object.__setattr__(self, "orig_t0", ensure_utc(self.orig_t0))
        object.__setattr__(self, "sim_start", ensure_utc(self.sim_start))

    def map_sim_time(self, orig_time: datetime) -> datetime:
        """Map original CSV timestamp to simulated event_time in UTC."""
        orig_utc = ensure_utc(orig_time)
        delta = orig_utc - self.orig_t0
        return self.sim_start + delta

    def target_wall_time(self, orig_time: datetime) -> float:
        """Compute absolute monotonic deadline when this event should be emitted."""
        delta_sec = (ensure_utc(orig_time) - self.orig_t0).total_seconds()
        return self.t_start_monotonic + (delta_sec / self.speed_factor)

    def behind_schedule_ms(self, orig_time: datetime, now_monotonic: float) -> float:
        """Return lag in milliseconds if now_monotonic passed target_wall_time, else 0.0."""
        lag_sec = now_monotonic - self.target_wall_time(orig_time)
        return max(0.0, lag_sec * 1000.0)
