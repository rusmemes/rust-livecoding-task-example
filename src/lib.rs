use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub enum SelectionStrategy {
    Random,
    LRU,
}

pub struct Wrapper {
    pool: Option<Arc<Mutex<ConnectionPoolStorage>>>,
    pub connection: Option<Arc<Connection>>,
    return_callback: Option<Box<dyn FnOnce(Arc<Mutex<ConnectionPoolStorage>>, Arc<Connection>)>>,
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        let callback = self.return_callback.take();
        let pool = self.pool.take();
        let conn = self.connection.take();
        if let (Some(callback), Some(pool), Some(conn)) = (callback, pool, conn) {
            callback(pool, conn);
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    pub id: String,
}

struct ConnectionPoolStorage {
    cache: HashMap<String, Option<Arc<Connection>>>,
    value_added: Arc<Notify>,
}

#[derive(Clone)]
pub struct ConnectionPool {
    connection_pool_storage: Arc<Mutex<ConnectionPoolStorage>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            connection_pool_storage: Arc::new(Mutex::new(ConnectionPoolStorage {
                cache: HashMap::new(),
                value_added: Arc::new(Notify::new()),
            })),
        }
    }

    pub async fn add(&mut self, key: String, conn: Connection) {
        ConnectionPoolStorage::add(self.connection_pool_storage.clone(), key, Arc::new(conn)).await
    }

    pub async fn remove(&mut self, id: &str) -> Option<Arc<Connection>> {
        self.connection_pool_storage.lock().await.remove(id).await
    }

    pub async fn get(&mut self, selection_strategy: &SelectionStrategy) -> Wrapper {
        ConnectionPoolStorage::get(self.connection_pool_storage.clone(), selection_strategy).await
    }
}

impl ConnectionPoolStorage {
    async fn add(pool: Arc<Mutex<ConnectionPoolStorage>>, key: String, conn: Arc<Connection>) {
        let mut guard = pool.lock().await;
        guard.cache.insert(key, Some(conn));
        guard.value_added.notify_one();
    }

    async fn return_to_pool(
        pool: Arc<Mutex<ConnectionPoolStorage>>,
        key: String,
        conn: Arc<Connection>,
    ) {
        let mut guard = pool.lock().await;
        if let Some(option) = guard.cache.get_mut(&key) {
            if option.is_none() {
                let _ = option.insert(conn);
                guard.value_added.notify_one();
            }
        }
    }

    async fn remove(&mut self, id: &str) -> Option<Arc<Connection>> {
        self.cache.remove(id).flatten()
    }

    async fn get(
        pool: Arc<Mutex<ConnectionPoolStorage>>,
        selection_strategy: &SelectionStrategy,
    ) -> Wrapper {
        let key;
        let conn;
        loop {
            let mut guard = pool.lock().await;
            if let Some((k, c)) = guard.select_conn(selection_strategy) {
                key = k;
                conn = c;
                break;
            }
            let arc = guard.value_added.clone();
            drop(guard);
            arc.notified().await;
        }

        Wrapper {
            pool: Some(pool),
            connection: Some(conn),
            return_callback: Some(Box::new(|pool, conn| {
                tokio::spawn(ConnectionPoolStorage::return_to_pool(pool, key, conn));
            })),
        }
    }

    fn select_conn(
        &mut self,
        _selection_strategy: &SelectionStrategy, // completely ignored for now
    ) -> Option<(String, Arc<Connection>)> {
        if self.cache.is_empty() {
            return None;
        }
        let mut k: Option<String> = None;
        for key in self.cache.keys() {
            let option = self.cache.get(key);
            if option.is_none() || option.unwrap().is_none() {
                continue;
            }
            k = Some(key.clone());
            break;
        }

        if let Some(key) = k {
            if let Some(option) = self.cache.get_mut(&key) {
                let option = option.take();
                return Some((key, option.unwrap()));
            }
        }

        None
    }
}
