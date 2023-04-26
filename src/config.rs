
pub struct Cassandra<'a> {
    pub nodes: &'a [&'a str],
    pub username: &'a str,
    pub password: &'a str,
    pub port: u16,
    pub total_tables: usize,
}

// Node connections
pub const APIKEYS: [&str; 2] = ["ABCDEFGHIJKLMNBOPQRSTUVWXYZ", "amazonspotinstance997152"];

// logging
pub const DISCORDWEBHOOK: Option<&str> = None; //'https://discord.com/api/webhooks/1026000711055589406/JOeLgSAy6DR7rAri3UMr7MA3xM866TNRmxyEImcvVF2GFXG8mG8m83sVI7LgwVqpGoCD'
pub const TELEGRAMTOKEN: Option<&str> = None; 
pub const TELEGRAMCHATID: Option<&str> = None; 

pub struct Config<'a> {
    pub settings: Settings<'a>,
    pub logging: Logging<'a>,
    pub cassandra: Cassandra<'a>,
}

pub struct Logging<'a> {
    pub discord_webhook: Option<&'a str>,
    pub telegram_token: Option<&'a str>,
    pub telegram_chatid: Option<&'a str>,
}

pub struct Settings<'a> {
    pub batch_size: usize,
    pub api_keys: &'a [&'a str],
    pub rest_threads: usize,
}

pub fn config() -> Config<'static> {
    todo!()
}