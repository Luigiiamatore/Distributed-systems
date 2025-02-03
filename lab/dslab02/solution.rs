use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{JoinHandle, spawn};

type Task = Box<dyn FnOnce() + Send>;

/// The thread pool.
pub struct Threadpool {
    workers: Vec<Option<JoinHandle<()>>>,
    task_queue: Arc<Mutex<VecDeque<Task>>>,
    shutdown: Arc<AtomicBool>,
}

impl Threadpool {
    /// Create a new thread pool with `workers_count` workers.
    pub fn new(workers_count: usize) -> Self {
        if workers_count == 0 {
            panic!("The number of workers must be greater than 0");
        }

        let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(workers_count);

        for _ in 0..workers_count {
            let queue_clone = Arc::clone(&task_queue);
            let shutdown_clone = Arc::clone(&shutdown);
            workers.push(Some(spawn(move || Self::worker_loop(queue_clone, shutdown_clone))));
        }

        Self {
            workers,
            task_queue,
            shutdown,
        }
    }

    /// Submit a new task to the pool.
    pub fn submit(&self, task: Task) {
        let mut queue = self.task_queue.lock().unwrap();
        queue.push_back(task);
    }

    /// Worker loop that processes tasks.
    fn worker_loop(task_queue: Arc<Mutex<VecDeque<Task>>>, shutdown: Arc<AtomicBool>) {
        loop {
            let task = {
                let mut queue = task_queue.lock().unwrap();
                queue.pop_front()
            };

            match task {
                Some(task) => task(),
                None => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::yield_now();  // Prevents busy-waiting when no tasks are available
                }
            }
        }
    }
}

impl Drop for Threadpool {
    /// Gracefully shut down the thread pool.
    ///
    /// Wait until all submitted tasks are executed and join all worker threads.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        for worker in &mut self.workers {
            if let Some(handle) = worker.take() {
                handle.join().unwrap();
            }
        }
    }
}
