use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub server: Settings,
    pub logging: Logging,
    pub db: DbOptions,
    pub auth: Auth,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Logging {
    pub discord_webhook: Option<String>,
    pub telegram_token: Option<String>,
    pub telegram_chatid: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Auth {
    pub password: String,
    pub username: String,
    pub api_keys: Vec<String>, //TODO - X-content ... api key
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Settings {
    pub api_addr: String,
    pub node_addrs: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DbOptions {
    pub default_batch_size: Option<usize>,
    pub replication_factor: Option<usize>,
    pub keyspaces: Vec<String>,
}

static mut CONFIG: Option<&'static Config> = None;

fn init_config() -> &'static Config {
    let conf = std::fs::read_to_string("./config.toml")
        .expect("could not find config or did not have permissions to open config");
    let conf = toml::from_str::<Config>(&conf)
        .expect("could not parse config as toml");
    let conf = Box::new(conf);
    let conf = Box::leak(conf);
    unsafe { CONFIG = Some(conf); }
    conf
}

pub fn get_config() -> &'static Config {
    match unsafe { CONFIG } {
        Some(conf) => conf,
        None => init_config(),
    }
}
