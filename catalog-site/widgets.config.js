// portolan-browser widgets for catalog.geomermaids.com, copied over the
// browser's own widgets.config.js at build time (build.sh). Hooks and the
// pre-defined widgets: docs/widgets.md in portolan-sdi/portolan-browser.
export default {
  // On every page, above the header: what to do with these datasets.
  "root-start": [
    {
      id: "AlertBox",
      props: {
        variant: "info",
        title: "Geobox",
        text: "Add any of these datasets to your own box, or query them in place from "
          + "DuckDB, QGIS or an AI assistant. [Open Geobox](https://geobox.cloud/)",
      },
    },
  ],
};
