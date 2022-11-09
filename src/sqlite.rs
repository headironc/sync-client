use rusqlite::{params, Statement};

use crate::task::Task;

pub type Pool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;
pub type Connection = r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>;

pub enum Queries {
    GetIncompleteTask,
    GetCompletedAndLatestTask,
    InsertTask(Task),
}

pub async fn execute(pool: &Pool, query: Queries) -> rusqlite::Result<Task> {
    let pool = pool.clone();

    let connection = pool.get().unwrap();

    match query {
        Queries::GetIncompleteTask => get_incomplete_task(&connection),
        Queries::GetCompletedAndLatestTask => get_completed_and_latest_task(&connection),
        Queries::InsertTask(task) => insert_task(&connection, task),
    }
}

fn get_incomplete_task(connection: &Connection) -> rusqlite::Result<Task> {
    let statement = connection
        .prepare("SELECT * FROM tasks WHERE RunHostName is null")
        .unwrap();

    get_rows_as_task(statement)
}

fn get_completed_and_latest_task(connection: &Connection) -> rusqlite::Result<Task> {
    let statement = connection
        .prepare("SELECT * FROM tasks WHERE EndTime is not null ORDER BY EndTime DESC LIMIT 1")
        .unwrap();

    get_rows_as_task(statement)
}

fn insert_task(connection: &Connection, task: Task) -> rusqlite::Result<Task> {
    let mut statement = connection
        .prepare("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .unwrap();

    statement
        .execute(params![
            task.job_name,
            task.task_id,
            task.status,
            task.typo,
            task.percent,
            task.max_num_run,
            task.ret_val,
            task.num_run,
            task.workload,
            task.run_hostname,
            task.run_username,
            task.start_time,
            task.end_time,
            task.last_msg,
            task.milestone,
        ])
        .expect("TODO: panic message");

    get_incomplete_task(connection)
}

fn get_rows_as_task(mut statement: Statement) -> rusqlite::Result<Task> {
    statement.query_row([], |row| {
        Ok(Task {
            job_name: row.get(0)?,
            task_id: row.get(1)?,
            status: row.get(2)?,
            typo: row.get(3)?,
            percent: row.get(4)?,
            max_num_run: row.get(5)?,
            ret_val: row.get(6)?,
            num_run: row.get(7)?,
            workload: row.get(8)?,
            run_hostname: row.get(9)?,
            run_username: row.get(10)?,
            start_time: row.get(11)?,
            end_time: row.get(12)?,
            last_msg: row.get(13)?,
            milestone: row.get(14)?,
        })
    })
}
