use links::{parse_any_link, Link};

pub fn to_browseable(s: &str) -> askama::Result<Option<String>> {
    Ok({
        if let Some(link) = parse_any_link(s) {
            match link {
                Link::AtUri(at_uri) => at_uri.strip_prefix("at://").map(|noproto| {
                    format!("https://atproto-browser-plus-links.vercel.app/at/{noproto}")
                }),
                Link::Did(did) => Some(format!(
                    "https://atproto-browser-plus-links.vercel.app/at/{did}"
                )),
                Link::Uri(uri) => Some(uri),
            }
        } else {
            None
        }
    })
}
