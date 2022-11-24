from collections.abc import Callable

class Task:
    def __init__(self, cmdline: list, max_time: int):
        self.cmdline = cmdline
        self.max_time = int(max_time)

    def cmd(self) -> str:
        return " ".join(self.cmdline)

class CopyFileTask(Task):
    def __init__(self, fromfile: str, tofile: str):
        super().__init__(["cp", fromfile, tofile], 0)

class Job:
    def __init__(self):
        self.tasks = []
    
    def fit(self, _: Task) -> bool:
        return True

    def add(self, task: Task):
        self.tasks.append(task);

    def script(self) -> str:
        f : Callable[[Task], str] = lambda x: x.cmd()
        return "\n".join(map(f, self.tasks))

def generate(tasks: list[Task], new_job: Callable[[], Job]) -> list[Job]:
    job = new_job()
    jobs = [job]
    for task in tasks:
        # make sure task fits into current job, otherwise add a new job
        if not job.fit(task):
            job = new_job()
            jobs.append(job)
        
        job.add(task)
    
    return jobs
