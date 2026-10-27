use futures::TryFutureExt as _;
use acyclic_stream::{postgres::PostgresStream, *};
use std::time::Instant;

pub async fn verify(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    const AGENTS: usize = 100_000;
    let start = Instant::now();
    let namespace = uuid::Uuid::new_v4().to_string();
    let provider = PostgresStream::connect(database, &namespace).await?;
    let (client, connection) = tokio_postgres::connect(database, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let metrics = tokio::runtime::Handle::current().metrics();
    let tasks = metrics.num_alive_tasks();
    let threads = std::fs::read_dir("/proc/self/task")?.count();
    for base in (0..AGENTS).step_by(MAX_ITEMS) {
        let mut conditions = Vec::new();
        let mut mutations = Vec::new();
        for index in base..(base + MAX_ITEMS).min(AGENTS) {
            let path = StreamPath::new(format!("agent-{index:06}"))?;
            conditions.push(CommitCondition::Absent { path: path.clone() });
            mutations.push(CommitMutation::Append {
                path,
                records: vec![OwnedBytes::new().into()],
            });
        }
        assert!(matches!(
            provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    conditions,
                    mutations,
                    idempotency_key: IdempotencyKey::new(format!("scale-{base}").into_bytes())?,
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
    }
    println!(
        "100,000 distinct stream paths created in {:?}",
        start.elapsed()
    );
    let mut subscriptions = Vec::with_capacity(AGENTS);
    for index in 0..AGENTS {
        subscriptions.push(
            provider
                .follow(acyclic_stream::FollowRequest { path: StreamPath::new(format!("agent-{index:06}"))?, from: 1, preceding: None, filter: FollowFilter::default(), progress: false }).map_ok(acyclic_stream::records_only)
                .await?,
        );
    }
    assert_eq!(metrics.num_alive_tasks(), tasks);
    assert_eq!(std::fs::read_dir("/proc/self/task")?.count(), threads);
    let connections: i64 = client.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 OR application_name=$2", &[
        &format!("acyclic-stream-query-{namespace}"), &format!("acyclic-stream-listener-{namespace}")
    ]).await?.get(0);
    assert_eq!(connections, 2);
    println!(
        "100,000 distinct streams + subscriptions: {:?}; provider connections=2; task count unchanged={tasks}; OS threads unchanged={threads}",
        start.elapsed()
    );
    println!(
        "{}",
        std::fs::read_to_string("/proc/self/status")?
            .lines()
            .filter(|line| line.starts_with("VmRSS:"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    drop(subscriptions);
    driver.abort();
    Ok(())
}
