use fluent_uri::Uri;

pub mod at_uri;
pub mod did;
pub mod record;

pub use record::collect_links;

#[derive(Debug, PartialEq)]
pub enum Link {
    AtUri(String),
    Uri(String),
    Did(String),
}

impl Link {
    pub fn into_string(self) -> String {
        match self {
            Link::AtUri(s) => s,
            Link::Uri(s) => s,
            Link::Did(s) => s,
        }
    }
}

// normalizing is a bit opinionated but eh
pub fn parse_uri(s: &str) -> Option<String> {
    Uri::parse(s).map(|u| u.normalize().into_string()).ok()
}

pub fn parse_any_link(s: &str) -> Option<Link> {
    at_uri::parse_at_uri(s).map(Link::AtUri).or_else(|| {
        did::parse_did(s)
            .map(Link::Did)
            .or_else(|| parse_uri(s).map(Link::Uri))
    })
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
            parse_any_link("https://example.com"),
            Some(Link::Uri("https://example.com".into()))
        );

        assert_eq!(
            parse_any_link(
                "at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26"
            ),
            Some(Link::AtUri(
                "at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26".into()
            )),
        );

        assert_eq!(
            parse_any_link("did:plc:44ybard66vv44zksje25o7dz"),
            Some(Link::Did("did:plc:44ybard66vv44zksje25o7dz".into()))
        )
    }
}
