// portolan-browser options for catalog.geomermaids.com, merged over the
// browser's own config.js at build time (SB_CONFIG, read by build.sh).
// Options: docs/options.md in portolan-sdi/portolan-browser.
export default {
  catalogUrl: "https://catalog.geomermaids.com/catalog.json",
  catalogTitle: "Geomermaids catalogs",
  catalogImage: "/logo.png",
  // The children live on parquetry.geomermaids.com, another host.
  allowExternalAccess: true,
  historyMode: "hash",
  footerLinks: [
    { label: "geoparquet.geomermaids.com", url: "https://geoparquet.geomermaids.com/" },
    { label: "Source and issues", url: "https://github.com/gsueur/osm-geoparquet" },
  ],
};
