use livecoding_conn_pool::Pool;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct Connection {
    pub id: String,
}

#[tokio::main]
async fn main() {
    let mut pool = Pool::new();
    pool.add("1".to_string(), Connection { id: "1".to_string()}).await;

    for _ in 0..3 {
        {
            println!("{}", pool.get().await.id);
        } // here it's getting dropped
    }

    pool.remove(&"1".to_string()).await;

    let mut pool_clone = pool.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        pool_clone.add("2".to_string(), Connection { id: "2".to_string()}).await;
    });

    println!("{}", pool.get().await.id);
}