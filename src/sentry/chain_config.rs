use crate::{
    genesis::GenesisState,
    models::{ChainSpec, NetworkId, *},
};
use hashbrown::HashMap;

pub struct ChainsConfig(HashMap<String, ChainConfig>);

#[derive(Clone, Debug)]
pub struct ChainConfig {
    chain_spec: ChainSpec,
    genesis_block_hash: H256,
}

impl Default for ChainsConfig {
    fn default() -> Self {
        ChainsConfig::new()
    }
}

impl TryFrom<String> for ChainConfig {
    type Error = anyhow::Error;
    fn try_from(name: String) -> Result<Self, Self::Error> {
        ChainsConfig::default().get(&name)
    }
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
        self.chain_spec.gather_forks().into_iter().collect()
    }
}

impl ChainsConfig {
    pub fn new() -> Self {
        ChainsConfig(HashMap::from([
            (
                "mainnet".to_owned(),
                ChainConfig::new(crate::res::chainspec::MAINNET.clone()),
            ),
            (
                "ethereum".to_owned(),
                ChainConfig::new(crate::res::chainspec::MAINNET.clone()),
            ),
            (
                "ropsten".to_owned(),
                ChainConfig::new(crate::res::chainspec::ROPSTEN.clone()),
            ),
            (
                "rinkeby".to_owned(),
                ChainConfig::new(crate::res::chainspec::RINKEBY.clone()),
            ),
        ]))
    }

    pub fn get(&self, chain_name: &str) -> anyhow::Result<ChainConfig> {
        self.0
            .get(chain_name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unsupported chain: {}", chain_name))
    }
}

impl ChainsConfig {
    pub fn chain_names(&self) -> Vec<&String> {
        self.0.keys().collect()
    }
}
