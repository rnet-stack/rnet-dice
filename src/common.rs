use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;

pub fn set_bootstrap_node(addr: &str) -> Result<()> {
    let env_path = ".env";
    let content = fs::read_to_string(env_path).unwrap_or_default();
    let mut found = false;

    let mut lines: Vec<String> = content
        .lines()
        .map(|line| {
            if line.starts_with("BOOTSTRAP_NODE=") {
                found = true;
                format!("BOOTSTRAP_NODE={}", addr)
            } else {
                line.to_string()
            }
        })
        .collect();

    if !found {
        lines.push(format!("BOOTSTRAP_NODE={}", addr));
    }

    fs::write(env_path, lines.join("\n"))?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub enum MpcMsgType {
    General(String),
    Session(Vec<u8>),
    Advertize(String),
    Bootmesh(Vec<u8>),
}
