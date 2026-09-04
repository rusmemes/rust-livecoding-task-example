use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

struct GuardState {
    key: String,
    pool: Arc<Mutex<ConnectionPoolStorage>>,
    connection: Arc<Connection>,
}

pub struct Guard {
    state: Option<GuardState>,
}

impl Guard {
    pub fn connection(&self) -> &Connection {
        self.state
            .as_ref()
            .map(|state| state.connection.as_ref())
            .expect("Guard must be initialized")
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(GuardState {
            key,
            pool,
            connection,
        }) = self.state.take()
        {
            tokio::spawn(ConnectionPoolStorage::return_to_pool(pool, key, connection));
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

    pub async fn remove(&mut self, key: &str) -> Option<Arc<Connection>> {
        self.connection_pool_storage.lock().await.remove(key).await
    }

    pub async fn get(&mut self) -> Guard {
        ConnectionPoolStorage::get_next_available(self.connection_pool_storage.clone()).await
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

    async fn remove(&mut self, key: &str) -> Option<Arc<Connection>> {
        self.cache.remove(key).flatten()
    }

    async fn get_next_available(pool: Arc<Mutex<ConnectionPoolStorage>>) -> Guard {
        let key;
        let connection;
        loop {
            let mut guard = pool.lock().await;
            if let Some((k, c)) = guard.select_conn() {
                key = k;
                connection = c;
                break;
            }
            let arc = guard.value_added.clone();
            drop(guard);
            arc.notified().await;
        }

        Guard {
            state: Some(GuardState {
                key,
                pool,
                connection,
            }),
        }
    }

    fn select_conn(&mut self) -> Option<(String, Arc<Connection>)> {
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
