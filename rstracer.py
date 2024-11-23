import subprocess
from time import sleep

import psutil


class SingletonMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


        # ..
class Rstracer(metaclass=SingletonMeta):

    def __init__(self, path="rstracer"):
        self.path = path
        self.process = None

    def __del__(self):
        if self.state() == "Running":
            self.stop()

    def launch(self):
        if not self.state() == "Running":
            self.process = subprocess.Popen(["sudo", self.path])
            return self.process.pid

    def state(self):
        if self.process is None:
            return "Not running"
        return "Running" if self.process.poll() is None else "Exited"

    def stop(self):
        if self.process is not None:
            child_processes = [
                proc for proc in psutil.process_iter(attrs=["pid", "ppid"]) if proc.info["ppid"] == self.process.pid
            ]
            for child in child_processes:
                subprocess.run(["sudo", "kill", "-SIGINT", str(child.info["pid"])])
            subprocess.run(["sudo", "kill", "-SIGINT", str(self.process.pid)])
            while self.state() != "Exited":
                sleep(1)