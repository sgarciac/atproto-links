µcosm links
===========

optimistically extract links from arbitrary atproto records, optionally resolving canonical representations and possibly validating StrongRefs.


status
------

not at all ready (yet)

---

as far as i can tell, atproto lexicons today don't follow much of a convention for referencing across documents: sometimes it's a StrongRef, sometimes it's a DID, sometimes it's a bare at-uri. lexicon authors choose any old link-sounding key name for the key in their document.

it's pretty messy so embrace the mess: atproto wants to be part of the web, so this library will also extract URLs and other URIs if you want it to. all the links.


why
---

the atproto firehose that bluesky sprays at you will contain raw _contents_ from peoples' pdses. these are isolated, decontextualized updates. it's very easy to build some kinds of interesting downstream apps off of this feed.

- bluesky posts (firesky, deletions, )
- blueksy post stats (emojis, )
- trending keywords ()

but bringing almost kind of _context_ into your project requires a big step up in complexity and potentially cost: you're entering "appview" territory. _how many likes does a post have? who follows this account?_

you own your atproto data: it's kept in your personal data repository (PDS) and noone else can write to it. when someone likes your post, they create a "like" record in their _own_ pds, and that like belongs to _them_, not to you/your post.

in the firehose you'll see a `app.bsky.feed.post` record created, with no details about who has liked it. then you'll see separate `app.bsky.feed.like` records show up for each like that comes in on that post, with no context about the post except a random-looking reference to it. storing these in order to do so is up to you!

**so, why**

everything is links, and they're a mess, but they all kinda work the same, so maybe some tooling can bring down that big step in complexity from firehose raw-content apps -> apps requiring any social context.

everything is links:

- likes
- follows
- blocks
- reposts
- quotes

some low-level things you could make from links:

- notification streams (part of ucosm)
- a global reverse index (part of ucosm)

i think that making these low-level services as easy to use as jetstream could open up pathways for building more atproto apps that operate at full scale with interesting features for reasonable effort at low cost to operate.


extracting links
---------------


- low-level: pass a &str of a field value and get a parsed link back

- med-level: pass a &str of record in json form and get a list of parsed links + json paths back. (todo: should also handle dag-cbor prob?)

- high-ish level: pass the json record and maybe apply some pre-loaded rules based on known lexicons to get the best result.

for now, a link is only considered if it matches for the entire value of the record's field -- links embedded in text content are not included. note that urls in bluesky posts _will_ still be extracted, since they are broken out into facets.


resolving / canonicalizing links
--------------------------------


### at-uris

every at-uri has at least two equivalent forms, one with a `DID`, and one with an account handle. the at-uri spec [illustrates this by example](https://atproto.com/specs/at-uri-scheme):

- `at://did:plc:44ybard66vv44zksje25o7dz/app.bsky.feed.post/3jwdwj2ctlk26`
- `at://bnewbold.bsky.team/app.bsky.feed.post/3jwdwj2ctlk26`

some applications, like a reverse link index, may wish to canonicalize at-uris to a single form. the `DID`-form is stable as an account changes its handle and probably the right choice to canonicalize to, but maybe some apps would actually perfer to canonicalise to handles?

hopefully atrium will make it easy to resolve at-uris.


### urls

canonicalizing URLs is more annoying but also a bit more established. lots of details.

- do we have to deal with punycode?
- follow redirects (todo: only permanent ones, or all?)
- check for rel=canonical http header and possibly follow it
- check link rel=canonical meta tag and possibly follow it
- do we need to check site maps??
- do we have to care at all about AMP?
- do we want anything to do with url shorteners??
- how do multilingual sites affect this?
- do we have to care about `script type="application/ld+json"` ???

ugh. is there a crate for this.


### relative uris?

links might be relative, in which case they might need to be made absolute before being useful. is that a concern for this library, or up to the user? (seems like we might not have context here to determine its absolute)


### canonicalizing

there should be a few async functions available to canonicalize already-parsed links.

- what happens if a link can't be resolved?


---

- using `tinyjson` because it's nice -- maybe should switch to serde_json to share deps with atrium?

- would use atrium for parsing at-uris, but it's not in there. there's a did-only version in the non-lib commands.rs. its identifier parser is strict to did + handle, which makes sense, but for our purposes we might want to allow unknown methods too?

    - rsky-syntax has an aturi
    - adenosyne also
    - might come back to these


-------

rocks

```bash
ROCKSDB_LIB_DIR=/nix/store/z2chn0hsik0clridr8mlprx1cngh1g3c-rocksdb-9.7.3/lib/ cargo build
```
