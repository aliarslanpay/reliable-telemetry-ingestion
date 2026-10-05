"""Deadline-bounded helpers for project-owned native service processes."""
import json
import os
from pathlib import Path
import queue
import secrets
import shutil
import socket
import sqlite3
import subprocess
import time
import sys
import shlex
import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[1]


class RuntimeBlocked(RuntimeError):
    pass


def wait_for(predicate, timeout=8, label="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("deadline exceeded: " + label)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Process:
    def __init__(self, args, logfile, env=None):
        self.logfile = Path(logfile)
        self.output = self.logfile.open("ab", buffering=0)
        self.log_offset = self.output.tell()
        self.log_end = None
        self.process = subprocess.Popen(args, stdout=self.output, stderr=subprocess.STDOUT, env=env)

    def stop(self, kill=False, expected_codes=(0, -15)):
        started = time.monotonic()
        if self.process.poll() is None:
            self.process.kill() if kill else self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
                if not kill:
                    raise AssertionError("graceful shutdown exceeded 3 seconds")
        self.output.close()
        self.log_end = self.logfile.stat().st_size
        if not kill and self.process.returncode not in expected_codes:
            raise AssertionError("process exit code " + str(self.process.returncode) + "; inspect its project-owned log")
        return time.monotonic() - started

    def entries(self):
        result = []
        with self.logfile.open("rb") as stream:
            stream.seek(self.log_offset)
            size = -1 if self.log_end is None else self.log_end - self.log_offset
            lines = stream.read(size).decode("utf-8", errors="replace").splitlines()
        for line in lines:
            try:
                result.append(json.loads(line))
            except ValueError:
                pass
        return result


class Broker:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.port = port()
        self.passwords = {u:secrets.token_hex(16) for u in ("sensor-a", "sensor-b", "worker-1", "worker-2")}
        if not shutil.which("mosquitto") or not shutil.which("mosquitto_passwd"):
            raise RuntimeError("Install mosquitto and mosquitto-clients for real broker checks")
        password_file = self.directory / "passwords"
        for i, (user, password) in enumerate(self.passwords.items()):
            subprocess.run(["mosquitto_passwd", "-b", *(["-c"] if i == 0 else []), str(password_file), user, password], check=True, capture_output=True)
        acl = self.directory / "acl"
        acl.write_text("pattern write telemetry/v1/%u/events\npattern read telemetry/v1/%u/acks\n" +
                       "".join("\nuser " + user + "\ntopic read telemetry/v1/+/events\ntopic read $share/ingestors/telemetry/v1/+/events\ntopic write telemetry/v1/+/acks\n" for user in ("worker-1", "worker-2")))
        self.config = self.directory / "mosquitto.conf"
        self.config.write_text(("user root\n" if os.geteuid() == 0 else "") +
            f"listener {self.port} 127.0.0.1\nallow_anonymous false\npassword_file {password_file}\nacl_file {acl}\n" +
            "persistence false\nmax_packet_size 2048\nmessage_size_limit 1024\nmax_inflight_messages 16\nmax_queued_messages 64\nlog_dest stdout\nlog_type all\n")
        self.process = None

    def start(self):
        self.process = Process(["mosquitto", "-c", str(self.config)], self.directory / "broker.log")
        def listening():
            if self.process.process.poll() is not None:
                raise RuntimeError("Broker startup failed; inspect broker.log")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    return True
            except OSError:
                return False
        wait_for(listening, label="broker listener")

    def stop(self):
        if self.process:
            self.process.stop()
            self.process = None


class Peer:
    def __init__(self, broker, username, subscriptions=(), client_id=None, allow_denied=False):
        self.messages = queue.Queue(maxsize=256)
        self.ready = False
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id or "check-" + secrets.token_hex(4), protocol=mqtt.MQTTv311)
        self.client.username_pw_set(username, broker.passwords[username])
        self.client.max_queued_messages_set(32)
        self.client.max_inflight_messages_set(16)
        self.client.reconnect_delay_set(1, 2)
        def connected(client, _, __, reason, ___):
            if reason != 0:
                return
            if subscriptions:
                client.subscribe([(topic, 1) for topic in subscriptions])
            else:
                self.ready = True
        def subscribed(_, __, ___, reasons, ____):
            self.ready = allow_denied or all(not r.is_failure for r in reasons)
        def message(_, __, msg):
            try:
                self.messages.put_nowait((msg.topic, bytes(msg.payload)))
            except queue.Full:
                pass
        self.client.on_connect = connected
        self.client.on_subscribe = subscribed
        self.client.on_message = message
        self.client.connect_async("127.0.0.1", broker.port, 10)
        self.client.loop_start()
        wait_for(lambda:self.ready, label="peer subscription")

    def publish(self, topic, payload, retain=False, qos=1):
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":"))
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise AssertionError("peer publish was not queued")
        info.wait_for_publish(timeout=2)
        if not info.is_published():
            raise AssertionError("broker PUBACK deadline")

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


def rows(path):
    if not Path(path).exists():
        return []
    try:
        with sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True, timeout=0.2) as conn:
            return conn.execute("SELECT stream,seq,wire,fingerprint,state FROM outbox ORDER BY stream,seq").fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []
        raise


def gateway(binary, broker, directory, device="sensor-a", **options):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "MQTT_USERNAME":device, "MQTT_PASSWORD":broker.passwords[device]}
    args = [str(binary), "run", "--db", str(directory / "outbox.db"), "--device", device]
    merged = {"port":broker.port, "duration_ms":120000, "stay":1, **options}
    for key, value in merged.items():
        args += ["--" + key.replace("_", "-"), str(value)]
    return Process(args, directory / "gateway.log", env)


class Postgres:
    def __init__(self, directory):
        if os.geteuid() == 0:
            raise RuntimeBlocked("PostgreSQL cannot run as root; use a normal Ubuntu user. Docker is a separate option for the demo.")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.data = self.directory / "data"
        self.port = port()
        self.running = False
        bins = sorted(Path("/usr/lib/postgresql").glob("*/bin/initdb"), key=lambda p:int(p.parent.parent.name), reverse=True)
        self.initdb = shutil.which("initdb") or (str(bins[0]) if bins else None)
        if not self.initdb:
            raise RuntimeBlocked("Install postgresql-16 for the native isolated integration runner")
        self.pg_ctl = str(Path(self.initdb).parent / "pg_ctl")
        self.admin_url = f"postgresql://telemetry@127.0.0.1:{self.port}/telemetry"
        self.worker_password = secrets.token_hex(16)
        self.worker_url = f"postgresql://telemetry_worker:{self.worker_password}@127.0.0.1:{self.port}/telemetry"

    def initialize(self):
        import psycopg
        subprocess.run([self.initdb, "-D", str(self.data), "-U", "telemetry", "--no-locale", "--encoding=UTF8", "--auth=trust"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=10)
        self.start()
        with psycopg.connect(f"postgresql://telemetry@127.0.0.1:{self.port}/postgres", autocommit=True, connect_timeout=2) as conn:
            conn.execute("CREATE DATABASE telemetry")
        environment = {**os.environ, "PYTHONPATH":str(ROOT / "ingestion"), "DATABASE_ADMIN_URL":self.admin_url,
            "WORKER_DB_PASSWORD":self.worker_password}
        subprocess.run([sys.executable, "-m", "telemetry_ingestion.migrate"], env=environment, check=True, capture_output=True, timeout=8)

    def start(self):
        options = f"-h 127.0.0.1 -p {self.port} -k {shlex.quote(str(self.directory))} -c fsync=on -c synchronous_commit=on"
        subprocess.run([self.pg_ctl, "-D", str(self.data), "-l", str(self.directory / "postgres.log"), "-o", options, "-w", "-t", "5", "start"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=8)
        self.running = True

    def stop(self):
        if self.running:
            subprocess.run([self.pg_ctl, "-D", str(self.data), "-m", "fast", "-w", "-t", "5", "stop"],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=8)
            self.running = False

    def records(self):
        import psycopg
        with psycopg.connect(self.worker_url, connect_timeout=2,
                options="-c statement_timeout=1500") as conn:
            return conn.execute("SELECT device_id,stream_id,sequence,fingerprint,data FROM events").fetchall()


def worker(broker, postgres, directory, name="worker-1", **hooks):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    environment = {**os.environ, "PYTHONPATH":str(ROOT / "ingestion"), "MQTT_USERNAME":name,
        "MQTT_PASSWORD":broker.passwords[name], "DATABASE_URL":postgres.worker_url}
    args = [sys.executable, "-m", "telemetry_ingestion.worker", "--host", "127.0.0.1", "--port", str(broker.port), "--name", name]
    if hooks:
        args += ["--test-hooks", str(directory / "hooks")]
        if hooks.get("pause"):
            args += ["--test-pause-before-commit"]
        if "after" in hooks:
            args += ["--test-after-commit", hooks["after"]]
        if "delay" in hooks:
            args += ["--test-delay-ms", str(hooks["delay"])]
    process = Process(args, directory / "worker.log", environment)
    wait_for(lambda:any(e.get("kind") == "subscription" and e.get("granted") for e in process.entries()), label=name + " shared subscription")
    return process
