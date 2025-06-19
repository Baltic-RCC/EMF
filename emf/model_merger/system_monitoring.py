import asyncio
import os
import threading
import time
from functools import wraps
from statistics import mean
import matplotlib.pyplot as plt

import pandas
import psutil
from psutil import cpu_percent, virtual_memory, Process


def process_memory():
    process = Process(os.getpid())
    memory_info = process.memory_info()
    cpu_percent_value = process.cpu_percent()
    return memory_info, cpu_percent_value


class MemoryAndOtherStuffMonitor:
    def __init__(self, new_pid=None, new_measurement_interval=0.1):
        self.continue_to_measure = False
        self.pid = new_pid,
        self.measurement_results = []
        self.measurement_interval = new_measurement_interval
        self.start_time = None

    def measure(self, pid=None, measurement_interval = None):
        self.pid = pid or self.pid
        if not self.pid:
            self.pid = os.getpid()
        self.continue_to_measure = True
        self.measurement_interval = measurement_interval or self. measurement_interval
        self.start_time = time.time()
        if isinstance(self.pid, tuple):
            self.pid = self.pid[0]
        process = Process(self.pid)
        while self.continue_to_measure:
            cpu_used = process.cpu_percent(interval=self.measurement_interval)
            memory_used = process.memory_info().rss / (1024 * 1024)
            time_taken = time.time() - self.start_time
            self.measurement_results.append({'Time': time_taken, 'Memory': memory_used, 'CPU': cpu_used})
            time.sleep(self.measurement_interval)

    def get_results(self):
        return pandas.DataFrame(self.measurement_results)


def memory_profiler(func):
    @wraps(wrapped=func)
    def wrapper(*args, **kwargs):
        result = None
        current_pid = os.getpid()
        measurement_thingy = MemoryAndOtherStuffMonitor(new_pid=current_pid)
        thread = threading.Thread(target=measurement_thingy.measure)
        thread.start()
        response = func(*args, **kwargs)
        measurement_thingy.continue_to_measure = False
        thread.join()
        results = measurement_thingy.get_results()
        plt.figure(figsize=(10, 6))
        plt.subplot(2, 1, 1)
        plt.plot(results['Time'], results['CPU'], label='CPU usage (%)')
        plt.title('CPU Usage Over Time')
        plt.title('Time (seconds)')
        plt.ylabel('CPU usage (%)')

        plt.subplot(2, 1, 2)
        plt.plot(results['Time'], results['Memory'], label='Memory usage (MB)')
        plt.title('Memory Usage Over Time')
        plt.title('Time (seconds)')
        plt.ylabel('Memory usage (MB)')

        plt.tight_layout()
        plt.show()

        print("Done")
    return wrapper


def monitor_function_usage(func, *args, **kwargs):
    process = psutil.Process()
    cpu_usage = []
    memory_usage = []
    start_time = time.time()
    interval = 0.1


class SystemMonitoring:
    def __init__(self) -> None:
        self.main_path = None
        self.execution_stats: list = []
        self._cpu_usage_data: list = []
        self._ram_usage_data: list = []
        self._sampling_task = asyncio.Task
        self.max_cpu_used: float = 0
        self.avg_cpu_used: float = 0
        self.max_ram_used: float = 0
        self.avg_ram_used: float = 0
        self.duration: float = 0
        self.start_time: float = 0
        self.finish_time: float = 0
        self.stats: dict = {}
        self._sampling: bool = False
        self.baseline_cpu: float = 0
        self.baseline_ram: float = 0
        self.pid = None
        self.all_measurements = []

    def set_pid(self, pid):
        self.pid = pid

    async def _get_current_state(self, interval: float = 0.1):
        self._sampling = True
        while self._sampling:
            try:
                time_of_measurement = time.time()
                if not self.pid:
                    self.pid = os.getpid()
                process = Process(self.pid)
                results = {'Time': time_of_measurement,
                           'Memory': float(process.memory_info().rss),
                           'CPU': float(process.cpu_percent())}
                print(len(self.all_measurements))
                self.all_measurements.append(results)
                self._cpu_usage_data.append(cpu_percent())
                self._ram_usage_data.append(virtual_memory().percent)
                await asyncio.sleep(interval)
            except Exception as exc:
                print(f"Measuring cpu and ram failed due to {exc}")
                raise exc

    async def _calculate_stats(self):
        self.max_cpu_used = max(self._cpu_usage_data)
        self.avg_cpu_used = mean(self._cpu_usage_data)
        self.max_ram_used = max(self._ram_usage_data)
        self.avg_ram_used = mean(self._ram_usage_data)

    async def _get_baseline_state(self):
        self.baseline_cpu = cpu_percent(0.5)
        self.baseline_ram = virtual_memory().percent

    async def start(self, interval: float = 0.1) -> bool:
        try:
            if not self._sampling:
                await self._get_baseline_state()
                self.start_time = time.time()
                self._sampling_task = asyncio.create_task(self._get_current_state(interval))
                return True
        except Exception as exc:
            print(f"System monitoring failed to start due to {exc}")
            raise exc
        return False

    async def stop(self):
        try:
            if self._sampling:
                self._sampling = False
                await self._sampling_task

                self.finish_time = time.time()
                self.duration = self.finish_time - self.start_time
                await self._calculate_stats()

                self.stats.update(
                    {
                        "duration": round(float(self.duration), 2),
                        "max_cpu": round(float(self.max_cpu_used), 2),
                        "avg_cpu": round(float(self.avg_cpu_used), 2),
                        "max_ram": round(float(self.max_ram_used), 2),
                        "avg_ram": round(float(self.avg_ram_used), 2),
                        "baseline_cpu": round(float(self.baseline_cpu), 2),
                        "baseline_ram": round(float(self.baseline_ram), 2)
                    }
                )
                if self.all_measurements:
                    dataframe = pandas.DataFrame(self.all_measurements)
                    return dataframe
                return self.stats
            else:
                return {}
        except Exception as exc:
            print(f"System monitoring failed to stop due to {exc}")
            raise exc
