// Bundles each SQS consumer into one self-contained CJS file and zips it for
// Lambda. esbuild follows the imports, so the zip holds the handler, the
// ClickHouse client and `undici` — and nothing from the Fastify edge, which the
// handlers never import.
//
// Run with `node scripts/lambda/build.mjs`; output lands in dist-lambda/.
import { execFileSync } from 'node:child_process'
import { mkdirSync, rmSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..')
const outDir = resolve(root, 'dist-lambda')

const handlers = [
  { name: 'pending', entry: 'src/workers/pending.handler.ts' },
  { name: 'accepted', entry: 'src/workers/accepted.handler.ts' },
]

rmSync(outDir, { recursive: true, force: true })

for (const { name, entry } of handlers) {
  const dir = resolve(outDir, name)
  mkdirSync(dir, { recursive: true })
  // `.mjs` so the Lambda nodejs22.x runtime loads it as ESM and reads the named
  // `handler` export. Bundling to CJS instead loses the export under undici's
  // own `module.exports` assignment.
  const outfile = resolve(dir, 'index.mjs')

  await build({
    entryPoints: [resolve(root, entry)],
    outfile,
    bundle: true,
    platform: 'node',
    target: 'node22',
    format: 'esm',
    minify: true,
    sourcemap: false,
    // esbuild's ESM output uses these Node built-ins for CJS interop shims
    // (undici is CJS); banner-injecting them avoids a "require is not defined".
    banner: {
      js: "import { createRequire as __cr } from 'node:module'; const require = __cr(import.meta.url);",
    },
  })

  // Zip the single file at the archive root, so the handler path is `index`.
  execFileSync('zip', ['-j', '-q', resolve(outDir, `${name}.zip`), outfile])
  console.log(`built ${name} -> dist-lambda/${name}.zip`)
}
