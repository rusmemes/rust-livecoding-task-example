use livecoding_conn_pool::{Connection, ConnectionPool};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let mut pool = ConnectionPool::new();
    pool.add("1".to_string(), Connection { id: "1".to_string()}).await;

    for _ in 0..3 {
        {
            println!("{}", pool.get().await.connection().id);
        } // here it's getting dropped
    }

    pool.remove("1").await;

    let mut pool_clone = pool.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        pool_clone.add("2".to_string(), Connection { id: "2".to_string()}).await;
    });

    println!("{}", pool.get().await.connection().id);
}