use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use clickhouse::{Client, Row};
use rand::Rng;
use tokio::time::{Instant, sleep};

#[derive(Debug, Row, Serialize, Deserialize,Default)]
pub struct LogMoneyGameSpin {
    pub time:i64,
    pub account_id:i32,
    pub diff_money:f64,
    pub diff_money_gift:f64,
    pub log_game_in_id:i64,
    pub game_id:i32,
    pub service_id:i32,
    pub bet_money:f64,
    pub win_money:f64,
    pub lock_win_money:f64,
    pub r#type:i32,
    pub money :f64,
    pub money_safe :f64,
    pub money_gift :f64,
    pub money_gift_safe :f64,
    pub amount_of_gift :f64,
    #[serde(rename="amount_of_washcode")]
    pub amount_of_wash_code:f64,
    pub money_type :i32,
    #[serde(rename="levelup_money")]
    pub level_up_money :i64,
    #[serde(rename="consum_money")]
    pub consume_money :f64,
    #[serde(rename="consum_money_gift")]
    pub consume_money_gift :f64,
    #[serde(rename="amount_of_slotswash")]
    pub amount_of_slots_wash :f64,
    #[serde(rename="consum_money_gift_of_slots")]
    pub consume_money_gift_of_slots :f64,
    #[serde(rename="consum_money_of_slots")]
    pub consume_money_of_slots :f64,
    pub income_money :f64,
    pub income_money_of_slots :f64
}

static TPS:AtomicU64=AtomicU64::new(0);

#[tokio::main]
async fn main()->Result<()> {
    let client = Client::default().with_user("default")
        .with_password("a123123")
        .with_database("test")
        .with_url("http://192.168.1.221:8123");
    //let mut table = client.insert("log_money_game_spin")?;

    // table.write(&LogMoneyGameSpin{
    //     time:chrono::prelude::Utc::now().timestamp_millis(),
    //     account_id:1,
    //     amount_of_wash_code:100f64,
    //     ..Default::default()
    // }).await?;
    // table.end().await?;

    client.query("ALTER TABLE log_money_game_spin DELETE WHERE `time`!=0").execute().await?;

    tokio::spawn(async move{
        loop {
            let tps= TPS.swap(0,Ordering::Release);
            println!("TPS:{tps}");
            sleep(Duration::from_secs(1)).await
        }
    });

    let mut inserter = client.inserter("log_money_game_spin")?
        .with_max_entries(5000)
        .with_period(Some(Duration::from_secs(10)));


    let mut rng =rand::prelude::thread_rng();

    let start=Instant::now();
    let x=LogMoneyGameSpin {
        time: chrono::prelude::Utc::now().timestamp_nanos(),
        account_id: 1,
        diff_money: rng.gen(),
        diff_money_gift: rng.gen(),
        log_game_in_id: rng.gen_range(0..1000),
        game_id: rng.gen_range(0..100),
        service_id: 101,
        bet_money: rng.gen(),
        win_money: rng.gen(),
        lock_win_money: rng.gen(),
        r#type: 1,
        money: rng.gen(),
        money_safe: rng.gen(),
        money_gift: rng.gen(),
        money_gift_safe: rng.gen(),
        amount_of_gift: rng.gen(),
        amount_of_wash_code: rng.gen(),
        money_type: 1,
        level_up_money: rng.gen(),
        consume_money: rng.gen(),
        consume_money_gift: rng.gen(),
        amount_of_slots_wash: rng.gen(),
        consume_money_gift_of_slots: rng.gen(),
        consume_money_of_slots: rng.gen(),
        income_money: rng.gen(),
        income_money_of_slots: rng.gen(),
    };
    println!("LogMoneyGameSpin create {} sec",start.elapsed().as_secs_f32());
    inserter.write(&x).await?;


    for _ in 0..100000000 {
        inserter.write(&LogMoneyGameSpin {
            time: chrono::prelude::Utc::now().timestamp_nanos(),
            account_id: 1,
            diff_money: rng.gen(),
            diff_money_gift: rng.gen(),
            log_game_in_id: rng.gen_range(0..1000),
            game_id: rng.gen_range(0..100),
            service_id: 101,
            bet_money: rng.gen(),
            win_money: rng.gen(),
            lock_win_money: rng.gen(),
            r#type: 1,
            money: rng.gen(),
            money_safe: rng.gen(),
            money_gift: rng.gen(),
            money_gift_safe: rng.gen(),
            amount_of_gift: rng.gen(),
            amount_of_wash_code: rng.gen(),
            money_type: 1,
            level_up_money: rng.gen(),
            consume_money: rng.gen(),
            consume_money_gift: rng.gen(),
            amount_of_slots_wash: rng.gen(),
            consume_money_gift_of_slots: rng.gen(),
            consume_money_of_slots: rng.gen(),
            income_money: rng.gen(),
            income_money_of_slots: rng.gen(),
        }).await?;

        // inserter.write(&LogMoneyGameSpin{
        //     time:chrono::prelude::Utc::now().timestamp_millis(),
        //     account_id:1,
        //     amount_of_wash_code:100f64,
        //     ..Default::default()
        // }).await?;
         inserter.commit().await?;
        TPS.fetch_add(1,Ordering::Release);
    }

    inserter.end().await?;


    client.query("ALTER TABLE log_money_game_spin DELETE WHERE `time`!=0").execute().await?;

    Ok(())
}
