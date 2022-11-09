use r2d2_sqlite::SqliteConnectionManager;

use sync_client::*;

#[tokio::main]
async fn main() {
    let current_dir = std::env::current_dir().unwrap();
    let sqlite_manager = SqliteConnectionManager::file(current_dir.join("job-queue.db"));
    let sqlite_pool = sqlite::Pool::new(sqlite_manager).unwrap();

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        let task = task::get_incomplete_task(sqlite_pool.clone()).await;
        println!("task: {:#?}", task);
    }
}
