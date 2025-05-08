pub const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>UFOs API Documentation</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="API Documentation for UFOs: Samples and stats for all atproto lexicons." />
    <style>
      .custom-header {
        height: 42px;
        background-color: var(--scalar-background-1);
        box-shadow: inset 0 -1px 0 var(--scalar-border-color);
        color: var(--scalar-color-1);
        font-size: var(--scalar-font-size-3);
        font-family: 'Iowan Old Style', 'Palatino Linotype', 'URW Palladio L', P052, serif;
        padding: 0 18px;
        justify-content: space-between;
      }
      .custom-header,
      .custom-header nav {
        display: flex;
        align-items: center;
        gap: 18px;
      }
      .custom-header a:hover {
        color: var(--scalar-color-2);
      }
    </style>
  </head>
  <body>
    <header class="custom-header scalar-app">
      <b>a <a href="https://microcosm.blue">microcosm</a> project</b>
      <nav>
        <a href="https://bsky.app/profile/microcosm.blue">@microcosm.blue</a>
        <a href="https://github.com/at-microcosm">github</a>
      </nav>
    </header>

    <script id="api-reference" type="application/json" data-url="/openapi""></script>

    <script>
      var configuration = {
        theme: 'purple',
      }
      document.getElementById('api-reference').dataset.configuration = JSON.stringify(configuration)
    </script>

    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>
"#;
