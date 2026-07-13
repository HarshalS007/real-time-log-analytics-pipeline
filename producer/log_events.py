"""
Synthetic log event generator.

Produces realistic-looking application log events across a handful of
microservices, including periodic "incidents" — a service that temporarily
spikes in error rate and latency — so the Grafana dashboards built on top of
this pipeline have something interesting to show (this is what "cut MTTD by
60%" is built to demonstrate: the P95/error-rate panels make an incident
visible within seconds instead of requiring someone to grep logs).
"""
import random
import socket
import time
import uuid

SERVICES = [
    "auth-service",
    "checkout-service",
    "payment-service",
    "inventory-service",
    "notification-service",
]

LEVEL_WEIGHTS = [("INFO", 0.78), ("WARN", 0.14), ("ERROR", 0.08)]

MESSAGES_BY_STATUS = {
    2: ["request completed", "handled successfully", "ok"],
    4: ["invalid request payload", "unauthorized", "rate limit exceeded", "not found"],
    5: ["upstream timeout", "database connection error", "unhandled exception", "circuit breaker open"],
}

HOSTNAME = socket.gethostname()


class IncidentSimulator:
    """
    Tracks whether each service is currently in a simulated "incident" window
    (elevated error rate + latency). Incidents start randomly and self-heal
    after a random duration, mimicking a real deploy-induced regression.
    """

    def __init__(self, incident_chance_per_tick=0.0006, min_duration=20, max_duration=90):
        self.incident_chance = incident_chance_per_tick
        self.min_duration = min_duration
        self.max_duration = max_duration
        self._active_until = {s: 0.0 for s in SERVICES}

    def is_incident(self, service, now):
        if now < self._active_until.get(service, 0):
            return True
        if random.random() < self.incident_chance:
            self._active_until[service] = now + random.uniform(self.min_duration, self.max_duration)
            return True
        return False


def _weighted_choice(weights):
    r = random.random()
    upto = 0
    for value, weight in weights:
        upto += weight
        if r <= upto:
            return value
    return weights[-1][0]


def generate_event(incidents: IncidentSimulator, now=None):
    now = now if now is not None else time.time()
    service = random.choice(SERVICES)
    incident = incidents.is_incident(service, now)

    if incident:
        # During an incident: errors spike and latency degrades noticeably.
        level = _weighted_choice([("INFO", 0.35), ("WARN", 0.25), ("ERROR", 0.40)])
        status_code = random.choices([200, 429, 500, 503], weights=[0.35, 0.15, 0.35, 0.15])[0]
        latency_ms = max(5.0, random.gauss(650, 220))
    else:
        level = _weighted_choice(LEVEL_WEIGHTS)
        status_code = random.choices([200, 201, 400, 404, 500], weights=[0.88, 0.05, 0.04, 0.02, 0.01])[0]
        latency_ms = max(1.0, random.gauss(45, 20))

    status_bucket = status_code // 100
    if status_bucket == 5:
        level = "ERROR"
    elif status_bucket == 4 and level == "INFO":
        level = "WARN"

    message = random.choice(MESSAGES_BY_STATUS.get(status_bucket, ["request processed"]))

    return {
        "event_time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)) + f".{int((now % 1) * 1000):03d}",
        "service": service,
        "level": level,
        "status_code": status_code,
        "latency_ms": round(latency_ms, 2),
        "message": message,
        "host": HOSTNAME,
        "trace_id": str(uuid.uuid4()),
    }
