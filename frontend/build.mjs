import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const srcDir = path.join(root, 'public');
const distDir = path.join(root, 'dist');
const apiBase = String(process.env.API_BASE || '').trim().replace(/\/+$/, '');

fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });
fs.cpSync(srcDir, distDir, { recursive: true });
fs.writeFileSync(
  path.join(distDir, 'runtime-config.js'),
  `window.__API_BASE__ = ${JSON.stringify(apiBase)};\n`,
  'utf8',
);

console.log(`frontend build complete. API_BASE=${apiBase || '(empty)'}`);
