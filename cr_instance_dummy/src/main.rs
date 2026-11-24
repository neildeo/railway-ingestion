use std::time::Duration;

fn heartbeat_message() -> &'static str {
    "Cloud Run Instance dummy: deployed via CI/CD"
}

#[tokio::main]
async fn main() {
    loop {
        println!("{}", heartbeat_message());
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_message_is_correct() {
        assert_eq!(
            heartbeat_message(),
            "Cloud Run Instance dummy: deployed via CI/CD"
        );
    }
}
