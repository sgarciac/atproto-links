use fluent_uri::{Uri, UriRef};
use std::sync::LazyLock;

static BASE: LazyLock<Uri<&str>> = LazyLock::new(|| Uri::parse("https://example.com").unwrap());

// normalizing is a bit opinionated but eh
/// see "Full AT URI Syntax" at https://atproto.com/specs/at-uri-scheme
/// this parser is intentinonally lax: it should accept all valid at-uris, and
/// may accept some invalid at-uris.
///
/// at the moment this implementation is quite bad and incomplete
pub fn parse_at_uri(s: &str) -> Option<String> {
    // for now, just working through the rules laid out in the docs in order,
    // without much regard for efficiency for now.

    // The overall URI is restricted to a subset of ASCII characters
    if !s.is_ascii() {
        return None;
    }
    // // A-Za-z0-9 . - _ ~
    // if !s.chars().all(|c| matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '-' | '_' | '~')) {
    //     return None
    // }

    // Maximum overall length is 8 kilobytes (which may be shortened in the future)
    if s.len() > (8 * 2_usize.pow(10)) {
        return None;
    }

    // Hex-encoding of characters is permitted (but in practice not necessary)
    // -> decode any unreserved characters. from rfc 3986:
    // ->   For consistency, percent-encoded octets in the ranges of ALPHA
    // ->   (%41-%5A and %61-%7A), DIGIT (%30-%39), hyphen (%2D), period (%2E),
    // ->   underscore (%5F), or tilde (%7E) should not be created by URI
    // ->   producers and, when found in a URI, should be decoded to their
    // ->   corresponding unreserved characters by URI normalizers.
    let s = if let Some((unencoded_prefix, rest)) = s.split_once('%') {
        let mut out = String::with_capacity(s.len());
        out.push_str(unencoded_prefix);
        for segment in rest.split('%') {
            let Some((hex2, unencoded_suffix)) = segment.split_at_checked(2) else {
                return None; // bail: % must always be followed by 2 hex digits
            };
            let Ok(decoded) = u8::from_str_radix(hex2, 16).map(char::from) else {
                return None; // bail: % must be followed by decodable hex
            };
            if matches!(decoded, 'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '-' | '_' | '~') {
                out.push(decoded);
            } else {
                out.push('%');
                out.push_str(&hex2.to_ascii_uppercase()); // norm
            }
            out.push_str(unencoded_suffix);
        }
        out
    } else {
        s.to_string()
    };

    // The URI scheme is `at`, and an authority part preceded with double slashes is always
    // required, so the URI always starts at://
    // -> the spec doesn't explicitly say, but it seems like uri schemes are case-insensitive
    let (proto, rest) = s.split_at_checked(5)?;
    if !proto.eq_ignore_ascii_case("at://") {
        return None;
    }

    // An authority section is required and must be non-empty. the authority can be either an
    // atproto Handle, or a DID meeting the restrictions for use with atproto. note that the
    // authority part can not be interpreted as a host:port pair, because of the use of colon
    // characters (:) in DIDs. Colons and unreserved characters should not be escaped in DIDs,
    // but other reserved characters (including #, /, $, &, @) must be escaped.
    //      Note that none of the current "blessed" DID methods for atproto allow these
    //      characters in DID identifiers

    // An optional path section may follow the authority. The path may contain multiple segments
    // separated by a single slash (/). Generic URI path normalization rules may be used.

    // An optional query part is allowed, following generic URI syntax restrictions

    // An optional fragment part is allowed, using JSON Path syntax

    // -> work backwards from fragment, query, path -> authority
    let mut base = rest;
    let (mut fragment, mut query, mut path) = (None, None, None);
    if let Some((pre, f)) = base.split_once('#') {
        base = pre;
        fragment = Some(f);
    }
    if let Some((pre, q)) = base.split_once('?') {
        base = pre;
        query = Some(q);
    }
    if let Some((pre, p)) = base.split_once('/') {
        base = pre;
        path = Some(p);
    }
    let mut authority = base.to_string();

    if authority.is_empty() {
        return None;
    }

    // Normalization: Authority as handle: lowercased
    if !authority.starts_with("did:") {
        // lowercase handles
        authority.make_ascii_lowercase();
    }

    // Normalization: No trailing slashes in path part
    // Normalization: No duplicate slashes or "dot" sections in path part (/./ or /abc/../ for example)
    // -> be so lazy
    let path = match path {
        Some(p) => {
            let p = p.trim_end_matches('/');
            let uri_ref = UriRef::parse(p).ok()?; // fully bail if we can't parse path
            let resolved = uri_ref.resolve_against(&*BASE).unwrap(); // both fail conditions are specific to BASE
            let normalized = resolved.normalize().path().to_string();
            let without_trailing_slashes = normalized.trim_end_matches('/');
            Some(without_trailing_slashes.to_string())
        }
        None => None,
    };

    let mut out = format!("at://{authority}");
    if let Some(p) = path {
        // no need for `/` -- it's added by fluent_uri normalization
        out.push_str(&p);
    }
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    if let Some(f) = fragment {
        out.push('#');
        out.push_str(f);
    }

    Some(out)

    // there's a more normalization to do still. ugh.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_uri_parse() {
        for (case, expected, detail) in vec![
            ("", None, "empty"),
            (" ", None, "whitespace"),
            ("https://bad-example.com", None, "not at scheme"),
            ("at://µcosm.bad-example.com", None, "not ascii"),
            (
                "at://bad-example.com",
                Some("at://bad-example.com"),
                "handle, authority-only",
            ),
            (
                "at://did:plc:hdhoaan3xa3jiuq4fg4mefid",
                Some("at://did:plc:hdhoaan3xa3jiuq4fg4mefid"),
                "DID, authority-only",
            ),
            (
                "at://bad-example.com/app.bsky.feed.post/3jwdwj2ctlk26",
                Some("at://bad-example.com/app.bsky.feed.post/3jwdwj2ctlk26"),
                "bsky post (handle)",
            ),
            (
                "at://did:plc:hdhoaan3xa3jiuq4fg4mefid/app.bsky.feed.post/3ldqksainxc27",
                Some("at://did:plc:hdhoaan3xa3jiuq4fg4mefid/app.bsky.feed.post/3ldqksainxc27"),
                "bsky post (DID)",
            ),
            (
                "AT://bad-example.com",
                Some("at://bad-example.com"),
                "scheme case is normalized",
            ),
            (
                "at://bad-example.com",
                Some("at://bad-example.com"),
                "scheme case is normalized",
            ),
            (
                "at://bad-example.com?q=z",
                Some("at://bad-example.com?q=z"),
                "query is allowed",
            ),
            (
                "at://bad-example.com#a",
                Some("at://bad-example.com#a"),
                "fragment is allowed",
            ),
            (
                "at://bad-example.com/%",
                None,
                "invalid percent-encoding: ends with %",
            ),
            (
                "at://bad-example.com/%2",
                None,
                "invalid percent-encoding: ends with only one digit after %",
            ),
            (
                "at://bad-example.com/%ZZ",
                None,
                "invalid percent-encoding: non-hex after %",
            ),
            (
                "at://bad-example.com/%3A",
                Some("at://bad-example.com/%3A"),
                "valid percent-encoding is left",
            ),
            (
                "at://bad-example.com/%3a",
                Some("at://bad-example.com/%3A"),
                "valid percent-encoding is hex-uppercased",
            ),
            (
                "at://bad-example.com/%61/%62",
                Some("at://bad-example.com/a/b"),
                "unreserved characters are percent-decoded",
            ),
            (
                "at://bad-example.com/a/../b",
                Some("at://bad-example.com/b"),
                "paths have traversals resolved (oof)",
            ),
            (
                "at://bad-example.com/../",
                Some("at://bad-example.com"),
                "paths always have trailing slashes removed",
            ),
        ] {
            assert_eq!(
                parse_at_uri(case),
                expected.map(|s| s.to_string()),
                "{detail}"
            );
        }
    }
}
