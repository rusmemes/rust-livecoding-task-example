use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub trait PoolKey: Eq + Hash + Clone + Send + 'static {}

impl<T> PoolKey for T where T: Eq + Hash + Clone + Send + 'static {}

pub trait PoolValue: Sync + Send + 'static {}

impl<T> PoolValue for T where T: Sync + Send + 'static {}

struct GuardState<K, V> {
    key: K,
    pool: Arc<Mutex<PoolStorage<K, V>>>,
    connection: Arc<V>,
}

pub struct Guard<K: PoolKey, V: PoolValue> {
    state: Option<GuardState<K, V>>,
}

impl<K: PoolKey, V: PoolValue> Guard<K, V> {
    pub fn connection(&self) -> &V {
        self.state
            .as_ref()
            .map(|state| state.connection.as_ref())
            .expect("Guard must be initialized")
    }
}

impl<K: PoolKey, V: PoolValue> Drop for Guard<K, V> {
    fn drop(&mut self) {
        if let Some(GuardState {
            key,
            pool,
            connection,
        }) = self.state.take()
        {
            tokio::spawn(PoolStorage::return_to_pool(pool, key, connection));
        }
    }
}

struct PoolStorage<K, V> {
    cache: HashMap<K, Option<Arc<V>>>,
    value_added: Arc<Notify>,
}

#[derive(Clone)]
pub struct Pool<K, V> {
    connection_pool_storage: Arc<Mutex<PoolStorage<K, V>>>,
}

impl<K: PoolKey, V: PoolValue> Pool<K, V> {
    pub fn new() -> Self {
        Self {
            connection_pool_storage: Arc::new(Mutex::new(PoolStorage {
                cache: HashMap::new(),
                value_added: Arc::new(Notify::new()),
            })),
        }
    }

    pub async fn add(&mut self, key: K, conn: V) {
        PoolStorage::add(self.connection_pool_storage.clone(), key, Arc::new(conn)).await
    }

    pub async fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        self.connection_pool_storage.lock().await.remove(key).await
    }

    pub async fn get(&mut self) -> Guard<K, V> {
        PoolStorage::get_next_available(self.connection_pool_storage.clone()).await
    }
}

impl<K: PoolKey, V: PoolValue> PoolStorage<K, V> {
    async fn add(pool: Arc<Mutex<PoolStorage<K, V>>>, key: K, conn: Arc<V>) {
        let mut guard = pool.lock().await;
        guard.cache.insert(key, Some(conn));
        guard.value_added.notify_one();
    }

    async fn return_to_pool(pool: Arc<Mutex<PoolStorage<K, V>>>, key: K, conn: Arc<V>) {
        let mut guard = pool.lock().await;
        if let Some(option) = guard.cache.get_mut(&key) {
            if option.is_none() {
                let _ = option.insert(conn);
                guard.value_added.notify_one();
            }
        }
    }

    async fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        self.cache.remove(key).flatten()
    }

    async fn get_next_available(pool: Arc<Mutex<PoolStorage<K, V>>>) -> Guard<K, V> {
        let key;
        let connection;
        loop {
            let mut guard = pool.lock().await;
            if let Some((k, c)) = guard.next() {
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

    fn next(&mut self) -> Option<(K, Arc<V>)> {
        if self.cache.is_empty() {
            return None;
        }

        let mut k: Option<K> = None;
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
