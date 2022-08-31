use std::{sync::Arc, time::Duration};

use graph::{
    components::store::{PruneReporter, StatusStore},
    data::subgraph::status,
    prelude::{anyhow, BlockNumber},
};
use graph_chain_ethereum::ENV_VARS as ETH_ENV;
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::manager::deployment::DeploymentSearch;

struct Progress;
impl PruneReporter for Progress {
    fn analyze(&mut self, table: &str) {
        println!("analyze {table}");
    }

    fn copy_nonfinal(&mut self, table: &str) {
        println!("copy nonfinal {table}");
    }

    fn copy_final_batch(
        &mut self,
        table: &str,
        rows: usize,
        total_rows: usize,
        elapsed: Duration,
        finished: bool,
    ) {
        print!(
            "\rcopy final entities for {table}: {rows} in {}s ({total_rows} total rows so far)",
            elapsed.as_secs()
        );
        if finished {
            println!("");
        }
    }

    fn finish_prune(&mut self, elapsed: Duration) {
        println!("finished pruning in {}s", elapsed.as_secs());
    }
}

pub async fn run(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    prune_ratio: f64,
) -> Result<(), anyhow::Error> {
    let history = history as BlockNumber;
    let deployment = search.locate_unique(&primary_pool)?;
    let mut info = store
        .status(status::Filter::DeploymentIds(vec![deployment.id]))?
        .pop()
        .ok_or_else(|| anyhow!("deployment {deployment} not found"))?;
    if info.chains.len() > 1 {
        return Err(anyhow!(
            "deployment {deployment} indexes {} chains, not sure how to deal with more than one chain",
            info.chains.len()
        ));
    }
    let status = info
        .chains
        .pop()
        .ok_or_else(|| anyhow!("deployment {} does not index any chain", deployment))?;
    let latest = status.latest_block.map(|ptr| ptr.number()).unwrap_or(0);
    if latest <= history {
        return Err(anyhow!("deployment {deployment} has only indexed up to block {latest} and we can't preserve {history} blocks of history"));
    }

    println!("prune {deployment} latest {latest}");

    let reporter = Box::new(Progress);
    store
        .subgraph_store()
        .prune(
            reporter,
            &deployment,
            latest - history,
            // Using the setting for eth chains is a bit lazy; the value
            // should really depend on the chain, but we don't have a
            // convenient way to figure out how each chain deals with
            // finality
            ETH_ENV.reorg_threshold,
            prune_ratio,
        )
        .await?;

    Ok(())
}
