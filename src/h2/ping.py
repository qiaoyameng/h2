"""
h2/ping
~~~~~~~

Types for PING probes whose acknowledgement is tracked by the connection.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class PingProbeStatus(Enum):
    """
    Terminal status of a PING probe.
    """

    ACKNOWLEDGED = "acknowledged"
    TIMED_OUT = "timed out"
    CONNECTION_TERMINATED = "connection terminated"


@dataclass(kw_only=True)
class PingProbe:
    """
    A handle for a PING awaiting acknowledgement.
    """

    ping_data: bytes
    started_at: float
    timeout: float
    status: PingProbeStatus | None = None
    rtt: float | None = None

    @property
    def done(self) -> bool:
        return self.status is not None

    def _complete(
        self,
        status: PingProbeStatus,
        rtt: float | None = None,
    ) -> PingProbe:
        if self.done:
            return self
        self.status = status
        self.rtt = rtt
        return self
