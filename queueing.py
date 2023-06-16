from atomicqueue import AtomicQueue, EventHandler


class PushHandler:
    def __init__(self, client):
        self.client = client

    def handle(self, el):
        columns = [x for x in el.keys()]
        rows = [x for x in el.values()]

        self.client.insert_rows(
            "git.github_events", columns, rows, column_oriented=True
        )


class Queue:
    def __init__(self, client):
        self.queue = AtomicQueue(256)
        self.queue.handle_events_with(PushHandler(client))
        self.queue.start()

    def add(self, task):
        self.queue.publish_event(task)

    def stop(self):
        self.queue.stop()


# additional queue for comparison
class MyQueue:
    def __init__(self):
        self.queue = []
        self.current = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.current == len(self.queue):
            raise StopIteration
        else:
            self.current += 1
            return self.queue[self.current - 1]

    def add(self, task):
        self.queue.append(task)

    def remove(self):
        self.queue.pop()

    def clear(self):
        self.queue.clear()

    def size(self):
        return len(self.queue)
