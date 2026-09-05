use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

pub trait PoolKey: Eq + Hash + Clone + Send + 'static {}

impl<T> PoolKey for T where T: Eq + Hash + Clone + Send + 'static {}

pub trait PoolValue: Sync + Send + 'static {}

impl<T> PoolValue for T where T: Sync + Send + 'static {}

struct GuardState<K, V> {
    key: K,
    pool: Arc<Mutex<PoolStorage<K, V>>>,
    value: V,
}

pub struct Guard<K: PoolKey, V: PoolValue> {
    state: Option<GuardState<K, V>>,
}

impl<K: PoolKey, V: PoolValue> Guard<K, V> {
    pub fn value(&self) -> &V {
        self.state
            .as_ref()
            .map(|state| &state.value)
            .expect("Guard must be initialized")
    }

    pub fn value_mut(&mut self) -> &mut V {
        self.state
            .as_mut()
            .map(|state| &mut state.value)
            .expect("Guard must be initialized")
    }
}

impl<K: PoolKey, V: PoolValue> Deref for Guard<K, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl<K: PoolKey, V: PoolValue> DerefMut for Guard<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value_mut()
    }
}

impl<K: PoolKey, V: PoolValue> Drop for Guard<K, V> {
    fn drop(&mut self) {
        if let Some(GuardState { key, pool, value }) = self.state.take() {
            tokio::spawn(PoolStorage::return_to_pool(pool, key, value));
        }
    }
}

struct PoolStorage<K, V> {
    cache: HashMap<K, Option<V>>,
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

    pub async fn add(&mut self, key: K, value: V) {
        PoolStorage::add(&self.connection_pool_storage, key, value).await
    }

    pub async fn remove(&mut self, key: &K) -> Option<V> {
        PoolStorage::remove(&self.connection_pool_storage, key).await
    }

    pub async fn get(&mut self) -> Guard<K, V> {
        PoolStorage::get_next_available(self.connection_pool_storage.clone()).await
    }
}

impl<K: PoolKey, V: PoolValue> PoolStorage<K, V> {
    async fn add(pool: &Mutex<PoolStorage<K, V>>, key: K, value: V) {
        let mut guard = pool.lock().await;
        guard.cache.insert(key, Some(value));
        guard.value_added.notify_one();
    }

    async fn return_to_pool(pool: Arc<Mutex<PoolStorage<K, V>>>, key: K, value: V) {
        let mut guard = pool.lock().await;
        if let Some(option) = guard.cache.get_mut(&key) {
            if option.is_none() {
                let _ = option.insert(value);
                guard.value_added.notify_one();
            }
        }
    }

    async fn remove(pool: &Mutex<PoolStorage<K, V>>, key: &K) -> Option<V> {
        pool.lock().await.cache.remove(key).flatten()
    }

    async fn get_next_available(pool: Arc<Mutex<PoolStorage<K, V>>>) -> Guard<K, V> {
        let mut notify = None;
        let key;
        let value;
        loop {
            let mut guard = pool.lock().await;
            if let Some((k, c)) = guard.next() {
                key = k;
                value = c;
                break;
            }
            if notify.is_none() {
                notify = Some(guard.value_added.clone());
            }
            drop(guard);
            notify.as_ref().unwrap().notified().await
        }

        Guard {
            state: Some(GuardState { key, pool, value }),
        }
    }

    fn next(&mut self) -> Option<(K, V)> {
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
