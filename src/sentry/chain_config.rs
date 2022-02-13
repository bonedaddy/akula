use crate::{
    genesis::GenesisState,
    models::{ChainSpec, NetworkId, *},
};
use std::collections::HashMap;

pub struct ChainsConfig(HashMap<String, ChainConfig>);

#[derive(Clone, Debug)]
pub struct ChainConfig {
    chain_spec: ChainSpec,
    genesis_block_hash: H256,
}

impl ChainConfig {
    fn new(chain_spec: ChainSpec) -> Self {
        let genesis = GenesisState::new(chain_spec.clone());
        let genesis_header = genesis.header(&genesis.initial_state());
        let genesis_block_hash = genesis_header.hash();

        Self {
            chain_spec,
            genesis_block_hash,
        }
    }

    pub const fn network_id(&self) -> NetworkId {
        self.chain_spec.params.network_id
    }

    pub fn chain_name(&self) -> String {
        self.chain_spec.name.to_lowercase()
    }

    pub const fn chain_spec(&self) -> &ChainSpec {
        &self.chain_spec
    }

    pub const fn genesis_block_hash(&self) -> ethereum_types::H256 {
        self.genesis_block_hash
    }

    pub fn fork_block_numbers(&self) -> Vec<BlockNumber> {
        self.chain_spec.gather_forks().iter().copied().collect()
    }
}

impl Default for ChainsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainsConfig {
    pub fn new() -> Self {
        let mut configs = HashMap::<String, ChainConfig>::new();
        configs.insert(
            String::from("mainnet"),
            ChainConfig::new(crate::res::chainspec::MAINNET.clone()),
        );
        configs.insert(
            String::from("ethereum"),
            ChainConfig::new(crate::res::chainspec::MAINNET.clone()),
        );
        configs.insert(
            String::from("ropsten"),
            ChainConfig::new(crate::res::chainspec::ROPSTEN.clone()),
        );
        configs.insert(
            String::from("rinkeby"),
            ChainConfig::new(crate::res::chainspec::RINKEBY.clone()),
        );
        ChainsConfig(configs)
    }

    pub fn get(&self, chain_name: &str) -> anyhow::Result<ChainConfig> {
        self.0
            .get(&chain_name.to_lowercase())
            .cloned()
            .ok_or_else(|| anyhow::format_err!("unknown chain '{}'", chain_name))
    }
}

impl ChainsConfig {
    pub fn chain_names(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect::<Vec<&str>>()
    }
}
