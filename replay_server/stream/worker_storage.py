from typing import Dict, List

from replay_server.stream.base import ReplayWorkerBase

__all__ = ('WorkerStorage',)


class WorkerStorage:
    """
    Handle replay workers and data
    """
    online_workers: Dict[int, List[object]] = {}

    @classmethod
    def add_worker(cls, uid, worker_instance: ReplayWorkerBase):
        cls.online_workers.setdefault(uid, []).append(worker_instance)

    @classmethod
    def get_online_workers(cls, uid):
        return cls.online_workers[uid]

    @classmethod
    def get_all_workers(cls):
        return cls.online_workers

    @classmethod
    def remove_worker(cls, uid, worker_instance: ReplayWorkerBase):
        cls.online_workers[uid].remove(worker_instance)
