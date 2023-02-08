use anyhow::Result;
use clickhouse::{Client, Row};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[derive(Debug, Row, Serialize, Deserialize, Default)]
pub struct LogMoneyGameSpin {
    pub timestamp: i64,
    pub account_id: i32,
    pub log_game_in_id: i64,
    pub game_id: i32,
    pub service_id: i32,

    pub spin_type: i32,
    pub is_win:bool,
    pub bet_money: f64,
    pub win_money: f64,

    pub money_type: i32,
    pub money: f64,
    pub money_gift: f64,
    pub amount_of_gift: f64,
    pub amount_of_wash_code: f64,
    pub level_up_money: i64,
    pub amount_of_slots_wash_code: f64,
}

static TPS: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> Result<()> {
    let client = Arc::new(
        Client::default()
            .with_user("default")
            .with_password("a123123")
            .with_database("test")
            .with_url("http://192.168.1.221:8123"),
    );


    client
        .query("optimize table log_money_slot final")
        .execute()
        .await?;

    let cc = client.clone();
    tokio::spawn(async move {
        loop {
            let tps = TPS.swap(0, Ordering::Release);

            let start = Instant::now();
            if let Ok(count)=cc
                .query("select count(*) from log_money_slot")
                .fetch_one::<i64>()
                .await {
                println!("每秒写入速度:{tps} 当前表数据量:{count}");
                let sleep_time = (1000000000i128 - start.elapsed().as_nanos() as i128).max(100000000);
                sleep(Duration::from_nanos(sleep_time as u64)).await;
            }
        }
    });

    let mut inserter = client
        .inserter("log_money_slot")?
        .with_max_entries(5000)
        .with_period(Some(Duration::from_secs(10)));

    let mut rng = rand::prelude::thread_rng();

    let start = Instant::now();
    let x = LogMoneyGameSpin {
        timestamp: chrono::prelude::Utc::now().timestamp_nanos(),
        account_id: 1,
        log_game_in_id: rng.gen_range(0..1000),
        game_id: rng.gen_range(0..100),
        service_id: 101,
        spin_type: 1,
        is_win: true,
        bet_money: rng.gen(),
        win_money: rng.gen(),
        money: rng.gen(),
        money_gift: rng.gen(),
        amount_of_gift: rng.gen(),
        amount_of_wash_code: rng.gen(),
        money_type: 1,
        level_up_money: rng.gen(),
        amount_of_slots_wash_code: rng.gen(),
    };
    println!(
        "LogMoneyGameSpin create {} sec",
        start.elapsed().as_secs_f32()
    );
    inserter.write(&x).await?;

    for _ in 0..1000000000 {
        if let Err(err) = inserter
            .write(&LogMoneyGameSpin {
                timestamp: chrono::prelude::Utc::now().timestamp_nanos(),
                account_id: 1,
                log_game_in_id: rng.gen_range(0..1000),
                game_id: rng.gen_range(0..100),
                service_id: 101,
                spin_type: 1,
                is_win: true,
                bet_money: rng.gen(),
                win_money: rng.gen(),
                money: rng.gen(),
                money_gift: rng.gen(),
                amount_of_gift: rng.gen(),
                amount_of_wash_code: rng.gen(),
                money_type: 1,
                level_up_money: rng.gen(),
                amount_of_slots_wash_code: rng.gen(),
            })
            .await
        {
            println!("write:{}", err.to_string());
            inserter = client
                .inserter("log_money_slot")?
                .with_max_entries(5000)
                .with_period(Some(Duration::from_secs(10)));
        }

        if let Err(err) = inserter.commit().await {
            println!("{}", err.to_string());
            inserter = client
                .inserter("log_money_slot")?
                .with_max_entries(5000)
                .with_period(Some(Duration::from_secs(10)));
        } else {
            TPS.fetch_add(1, Ordering::Release);
        }
    }

    inserter.end().await?;

    client
        .query("optimize table log_money_slot final")
        .execute()
        .await?;

    Ok(())
}
