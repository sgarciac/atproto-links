use fluent_uri::Uri;

#[derive(Debug, PartialEq)]
pub enum Link {
    AtUri(String),
    Uri(String),
}

// normalizing is a bit opinionated
pub fn parse_at_uri(_s: &str) -> Option<String> {
    // TODO
    None
}

// normalizing is a bit opinionated
pub fn parse_uri(s: &str) -> Option<String> {
    Uri::parse(s).map(|u| u.normalize().into_string()).ok()
}

pub fn parse_any(s: &str) -> Option<Link> {
    parse_at_uri(s)
        .map(Link::AtUri)
        .or_else(|| parse_uri(s).map(Link::Uri))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_parse() {
        let s = "https://example.com";
        let uri = parse_uri(s).unwrap();
        assert_eq!(uri.as_str(), s);
    }

    #[test]
    fn test_uri_normalizes() {
        let s = "HTTPS://example.com/../";
        let uri = parse_uri(s).unwrap();
        assert_eq!(uri.as_str(), "https://example.com/");
    }

    #[test]
    fn test_uri_invalid() {
        assert!(parse_uri("https:\\bad-example.com").is_none());
    }

    #[test]
    fn test_any_parse() {
        assert_eq!(
            parse_any("https://example.com"),
            Some(Link::Uri("https://example.com".into()))
        );

        assert_eq!(
            parse_any("at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26"),
            Some(Link::AtUri(
                "at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26".into()
            )),
        );
    }
}
