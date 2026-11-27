mod stomp;

#[tokio::main]
async fn main() {
    println!("TRUST ingestor: bootstrap");
}

#[cfg(test)]
mod tests {
    #[test]
    fn bootstrap_test() {
        assert_eq!(2 + 2, 4);
    }
}
