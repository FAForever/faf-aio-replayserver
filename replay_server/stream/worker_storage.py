from typing import Dict, List

from replay_server.stream.base import ReplayWorkerBase

__all__ = ('WorkerStorage',)


class WorkerStorage:
    """
    Handle replay workers like reader and writer
    """
    online_workers: Dict[int, List[ReplayWorkerBase]] = {}

    @classmethod
    def add_worker(cls, uid: int, worker_instance: ReplayWorkerBase) -> None:
        cls.online_workers.setdefault(uid, []).append(worker_instance)

    @classmethod
    def get_online_workers(cls, uid: int) -> List[ReplayWorkerBase]:
        return cls.online_workers[uid]

    @classmethod
    def remove_worker(cls, uid: int, worker_instance: ReplayWorkerBase) -> None:
        if uid in cls.online_workers and worker_instance in cls.online_workers[uid]:
            cls.online_workers[uid].remove(worker_instance)
