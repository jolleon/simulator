from collections import defaultdict
from dataclasses import dataclass
import json
from queue import Empty
import sys
import traceback
from typing import Type, Union
import requests
import time
import threading
import multiprocessing
import os
from datetime import datetime
import signal
import random
import string


class OtelWorkload:
    SPAN_DATA = json.dumps(
    {
        "resourceSpans": [
        {
            "resource": {
            "attributes": [
                {
                "key": "service.name",
                "value": {
                    "stringValue": "test-with-curl"
                }
                }
            ]
            },
            "scopeSpans": [
            {
                "scope": {
                "name": "manual-test"
                },
                "spans": [
                {
                    "traceId": "71699b6fe85982c7c8995ea3d9c95df2",
                    "spanId": "3c191d03fa8be065",
                    "name": "spanitron",
                    "kind": 2,
                    "droppedAttributesCount": 0,
                    "events": [],
                    "droppedEventsCount": 0,
                    "status": {
                    "code": 1
                    }
                }
                ]
            }
            ]
        }
        ]
    }
    )

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def send_request(self):
        requests.post(
            url=self.endpoint,
            data=self.SPAN_DATA,
            headers={'Content-Type': 'application/json'},
        )


class LargeRequestWorkload:
    def __init__(self, endpoint, size=10000000):
        self.endpoint = endpoint
        letters = string.ascii_letters + string.digits
        self.body = ''.join(random.choice(letters) for _ in range(size))

    def send_request(self):
        requests.post(
            url=self.endpoint,
            data=self.body,
            headers={'Content-Type': 'application/json'},
        )


WORKLOADS = {
    'otel': OtelWorkload,
    'large': LargeRequestWorkload,
}


class WorkloadConfig:
    def __init__(
        self,
        load_type: str,
        endpoint: str,
        **kwargs
    ):
        self.workload_class = WORKLOADS[load_type]
        self.endpoint = endpoint
        self.kwargs = kwargs

    def create(self):
        return self.workload_class(self.endpoint, **(self.kwargs or {}))


t_local = threading.local()
t_local.name = "main"

def log(s):
    print(f"{time.time()%1000:.2f}[{os.getpid()}|{t_local.name:0>2}] {s}")


@dataclass
class ThreadStatus:
    start_time: float
    end_time: Union[float,None]
    last_report_time: float
    time: float
    requests_sent_since_last_report: int
    total_requests_sent: int


@dataclass
class ThreadError:
    time: float
    exception: Union[Exception, None]
    tb: Union[str, None]


@dataclass
class Message:
    pid: int
    tid: int
    mtype: str
    content: Union[ThreadStatus, ThreadError, None]


class Reporter:
    def __init__(self, reporting_queue):
        self.reporting_queue = reporting_queue
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.requests_sent = 0

    def report(self, new_requests_sent, end_time=None):
        self.requests_sent += new_requests_sent
        now = time.time()
        self.reporting_queue.put(
            Message(
                os.getpid(), t_local.name,
                'status',
                ThreadStatus(
                            self.start_time, end_time,
                            self.last_report_time, now,
                            new_requests_sent, self.requests_sent)
            )
        )
        self.last_report_time = now


def spam_thread(thread_name, workload_config, duration_s, reporting_queue, max_rps=2):
    t_local.name = thread_name
    try:
        requester = workload_config.create()
        # start timer after creating the requester as it might take some time
        start_time = time.time()
        reporter = Reporter(reporting_queue)
        reporter.report(0)
        while time.time() - start_time < duration_s:
            last_request_time = time.time()
            requester.send_request()
            reporter.report(1)
            next_request_time = last_request_time + 1/max_rps
            sleep_time = next_request_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
        reporter.report(0, time.time())
    except Exception as e:
        tb = traceback.format_exc()
        reporting_queue.put(
            Message(
                os.getpid(), t_local.name,
                'error',
                ThreadError(time.time(), e, tb)
            )
        )


def compute_rps(msgs, now, window):
    recent_msgs = [msg for msg in msgs if msg.time > now - window]
    if len(recent_msgs) == 0:
        return 0, 0
    # right now we get 1 message after each request (requests_sent_since_last_report will allways be 1)
    # so we can just sum them and use window which is more accurate than end-start at the beginning
    # start = min(msg.last_report_time for msg in recent_msgs)
    # end = max(msg.time for msg in recent_msgs)
    reqs = sum(msg.requests_sent_since_last_report for msg in recent_msgs)
    return reqs / window, len(recent_msgs)


def reporting_thread(duration_s, reporting_queue, log_interval=1):
    t_local.name = 'report'
    threads_msgs: defaultdict[tuple[int, int], list[ThreadStatus]] = defaultdict(list)
    threads_status: dict[tuple[int, int], ThreadStatus] = dict()
    threads_errors: dict[tuple[int, int], ThreadError] = dict()
    total_sent = 0
    peak_rps = 0
    total_threads = 0
    threads_running = 0
    threads_dead = 0
    start_time = None
    while True:
        try:
            msg = reporting_queue.get(True, 1)
            if start_time is None:
                # initialize start time here in case threads take a while to initialize
                start_time = time.time()
                next_log = start_time  # log immediately
        except Empty:
            if total_threads == 0:
                log('waiting for threads to start')
                continue
            if threads_running > 0 and time.time() - start_time < duration_s:
                log(f'queue is empty but {threads_running}/{total_threads} threads still running')
                continue
            else:
                log('finished')
                break

        if msg.mtype == 'status':
            status = msg.content
            threads_msgs[(msg.pid, msg.tid)].append(status)
            threads_status[(msg.pid, msg.tid)] = status
            total_sent += status.requests_sent_since_last_report
        
        if msg.mtype == 'error':
            threads_errors[(msg.pid, msg.tid)] = msg.content
            threads_dead += 1
            log(f"thread [{msg.pid}][{msg.tid}] errored with {msg.content.exception}")
            log(f"{msg.content.tb}")

        if msg.mtype == 'shutdown':
            log('shutdown received')
            break

        total_threads = len(threads_status)
        threads_running = len([ptid for (ptid, status) in threads_status.items() if status.end_time is None and ptid not in threads_errors])

        now = time.time()
        run_time = now - start_time
        if now > next_log:
            # compute rps for running threads
            rps = 0
            max_rps = 0
            for pt, msgs in threads_msgs.items():
                # if threads_status[pt].end_time is None:
                    t_rps, recent_msg = compute_rps(msgs, now, window=2)
                    rps += t_rps
                    max_rps = max(t_rps, max_rps)
                    # trim older msgs
                    threads_msgs[pt] = msgs[-recent_msg:]
            if rps > peak_rps:
                peak_rps = rps
            log(f"{threads_running}/{total_threads} 🧵 ({threads_dead} 💀), ⏱️ {run_time:.2f}/{duration_s}s, sent {total_sent}, rps {rps:.2f} (🧵 {max_rps:.2f})")
            next_log += log_interval

    run_time = time.time()-start_time
    # let other threads finish logging first
    time.sleep(1)
    log(f"Finished - errors: {len(threads_errors)}")
    for ptid, error in threads_errors.items():
        log(f"**** Error detail for thread {ptid}")
        log(f"**** Exception: {error.exception}")
        log(f"{error.tb}")
        log(f"****")
    log(f"total requests sent: {total_sent}, run time: {run_time:.2f}, avg: {total_sent/run_time:.2f}rps, peak: {peak_rps:.2f}rps")
    log(f"errors: {len(threads_errors)}")
    log(f"{datetime.utcfromtimestamp(start_time).isoformat()} START")
    for error in sorted(threads_errors.values(), key=lambda er: er.time):
        log(f"{datetime.utcfromtimestamp(error.time).isoformat()} {type(error.exception).__name__}: {error.exception}")
    log(f"{datetime.utcfromtimestamp(time.time()).isoformat()} END")
    


def spam(n_threads, workload_config, duration_s, reporting_queue=None, max_rps=150):
    threads = list()
    start_time = time.time()
    rt = None

    if reporting_queue is None:
        reporting_queue = multiprocessing.Queue()
        rt = threading.Thread(target=reporting_thread, args=(duration_s, reporting_queue,))
        rt.start()

    log(f"starting {n_threads} threads")
    for i in range(n_threads):
        t = threading.Thread(target=spam_thread, args=(i, workload_config, duration_s, reporting_queue, max_rps/n_threads))
        threads.append(t)
        t.start()

    # log(f"waiting for threads")
    for i, t in enumerate(threads):
        t.join()

    if rt is not None:
        rt.join()

    total_time = time.time() - start_time
    log(f"finished - total time {total_time:.2f}s")


def multiprocess_spam(n_process, n_threads, workload_config, duration_s, max_rps, processes, reporting_queue):
    log(f"Starting {n_process} processes with {n_threads} threads each")
    log(f"Endpoint: {workload_config.endpoint}")
    log(f"Duration: {duration_s}s, max_rps: {max_rps}")
    log(f"Workload: {workload_config.workload_class.__name__}, {workload_config.kwargs}")
    log("")
    rt = threading.Thread(target=reporting_thread, args=(duration_s, reporting_queue,))
    rt.start()

    for i in range(n_process):
        # log(f"starting process {i}")
        p = multiprocessing.Process(target=spam, args=(n_threads, workload_config, duration_s, reporting_queue, max_rps/n_process))
        processes.append(p)
        p.start()

    for i, p in enumerate(processes):
        log(f"waiting for process {i}")
        p.join()

    log(f"waiting for reporting thread")
    rt.join()

    log("done")


def signal_handler(processes, reporting_queue):
    def shutdown(sig, frame):
        print('You pressed Ctrl+C!')
        for p in processes:
            p.terminate()
        reporting_queue.put(Message(0, 0, 'shutdown', None))
    return shutdown


def start(n_process, n_threads, workload_config, duration_s, max_rps=150):
    processes = list()
    reporting_queue = multiprocessing.Queue()
    signal.signal(signal.SIGINT, signal_handler(processes, reporting_queue))
    multiprocess_spam(n_process, n_threads, workload_config, duration_s, max_rps, processes, reporting_queue)


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 7:
        print(f"Usage: python {args[0]} n_process n_threads duration_s max_rps load_type({'/'.join(WORKLOADS.keys())}) endpoint (optional: workload_args)")
        sys.exit(1)
    n_process = int(args[1])
    n_threads = int(args[2])
    duration_s = int(args[3])
    max_rps = int(args[4])
    load_type = args[5]
    endpoint = args[6]
    workload_args = {k: int(v) for k, v in [arg.split('=') for arg in args[7:]]}
    workload_config = WorkloadConfig(load_type, endpoint, **workload_args)
    start(n_process, n_threads, workload_config, duration_s, max_rps)
