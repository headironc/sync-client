use serde::{Deserialize, Serialize};

use crate::sqlite::{execute, Pool, Queries};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Task {
    pub job_name: String,
    pub task_id: String,
    pub status: u8,
    pub typo: u8,
    pub percent: f32,
    pub max_num_run: u8,
    pub ret_val: Option<i32>,
    pub num_run: u8,
    pub workload: f32,
    pub run_hostname: Option<String>,
    pub run_username: Option<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub last_msg: Option<String>,
    pub milestone: Option<String>,
}

pub async fn get_remote_task() -> reqwest::Result<Task> {
    reqwest::get("http://127.0.0.1:4000/task")
        .await
        .unwrap()
        .json::<Task>()
        .await
}

pub async fn update_task(task: Task) -> reqwest::Result<Task> {
    let client = reqwest::Client::new();

    client
        .patch("http://127.0.0.1:4000/task")
        .json(&task)
        .send()
        .await
        .unwrap()
        .json::<Task>()
        .await
}

pub async fn get_incomplete_task(pool: Pool) -> Task {
    let result = execute(&pool, Queries::GetIncompleteTask).await;

    match result {
        Ok(task) => task,
        Err(_) => {
            let completed_and_latest_task_result =
                execute(&pool, Queries::GetCompletedAndLatestTask).await;

            match completed_and_latest_task_result {
                Ok(completed_and_latest_task) => {
                    let result = update_task(completed_and_latest_task).await;

                    match result {
                        Ok(task) => {
                            let task = execute(&pool, Queries::InsertTask(task.clone()))
                                .await
                                .unwrap();

                            task
                        }
                        Err(_) => panic!("No task found"),
                    }
                }
                Err(_) => {
                    let remote_task_result = get_remote_task().await;

                    match remote_task_result {
                        Ok(remote_task) => {
                            let task = execute(&pool, Queries::InsertTask(remote_task.clone()))
                                .await
                                .unwrap();

                            task
                        }
                        Err(_) => panic!("No task found"),
                    }
                }
            }
        }
    }
}
