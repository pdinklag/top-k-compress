from jobs import *

class SlurmJob(Job):
    def __init__(self, cmdline: list[str], time_left: int):
        super().__init__(self)
        self.cmdline = cmdline
        self.time_left = int(time_left)

    def fit(self, task: Task) -> bool:
        return self.time_left >= task.max_time

    def add(self, task: Task):
        super().add(self, task)
        self.time_left -= task.max_time
    
    def cmd(self, script_filename: str) -> str:
        return " ".join(["sbatch"] + self.cmdline + [script_filename])

class LidoShortStd01Job(Job):
    def __init__(self):
        super().__init__(["-c", "cstd01", "-p", "short", "--exclusive"], 120)
