# YAML parser

yaml 2.9.1 (ISC), https://github.com/eemeli/yaml, bundled with esbuild 0.28.2.
Retained locally so static CI installs no dependencies. License: yaml.LICENSE.
Generate from the pinned npm package: esbuild node_modules/yaml/dist/index.js --bundle --format=cjs --platform=node --minify --legal-comments=inline --outfile=yaml.cjs
