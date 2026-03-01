/**
 * Dynamic endpoint listing script. Reads server.ts and route files to produce
 * a JSON and Markdown export of all HTTP endpoints, their paths, methods,
 * path/query/body params, response shape, and descriptions.
 *
 * Run from project root: npx ts-node scripts/list-endpoints.ts
 * Output: scripts/endpoints.json and scripts/endpoints.md
 */

import * as fs from 'fs';
import * as path from 'path';

const PROJECT_ROOT = path.resolve(__dirname, '..');
const SRC = path.join(PROJECT_ROOT, 'src');

interface ParamInfo {
  name: string;
  type: string;
  required?: boolean;
}

interface EndpointInfo {
  method: string;
  path: string;
  pathParams: string[];
  queryParams: ParamInfo[];
  bodyParams: ParamInfo[];
  responseShape: string;
  description: string;
  sourceFile: string;
}

interface RouteFromFile extends Omit<EndpointInfo, 'path'> {
  routePath: string;
}

// Parse server.ts for app.use(mountPath, routerName)
function getMounts(): { path: string; name: string }[] {
  const serverPath = path.join(SRC, 'server.ts');
  const content = fs.readFileSync(serverPath, 'utf-8');
  const mounts: { path: string; name: string }[] = [];
  // app.use('/api/members', members); or app.use("/api/squads", squads);
  const re = /app\.use\s*\(\s*['"]([^'"]+)['"]\s*,\s*(\w+)\s*\)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(content)) !== null) {
    mounts.push({ path: m[1], name: m[2] });
  }
  return mounts;
}

// Extract path params from route path (e.g. /:id/profile -> ['id'])
function getPathParams(routePath: string): string[] {
  const re = /:(\w+)/g;
  const out: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(routePath)) !== null) out.push(m[1]);
  return out;
}

// Get the handler code for this route (from router.METHOD('path' to next router. or export default)
function getHandlerSnippet(content: string, method: string, routePath: string): string {
  const methodLower = method.toLowerCase();
  const escaped = routePath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`router\\.${methodLower}\\s*\\(\\s*['"\`]${escaped}['"\`]`);
  const m = content.match(re);
  const start = m?.index ?? -1;
  if (start < 0) return '';
  const afterStart = content.slice(start);
  const nextRouter = afterStart.slice(1).search(/\nrouter\.(get|post|put|delete|patch)\s*\(/);
  const nextExport = afterStart.slice(1).search(/\nexport\s+default/);
  let end = afterStart.length;
  if (nextRouter >= 0) end = Math.min(end, nextRouter + 1);
  if (nextExport >= 0) end = Math.min(end, nextExport + 1);
  return afterStart.slice(0, end);
}

// Parse Joi.object({ key: Joi.type()... }) in snippet for body params
function parseBodyParams(snippet: string): ParamInfo[] {
  const params: ParamInfo[] = [];
  const objMatch = snippet.match(/Joi\.object\s*\(\s*\{([\s\S]*?)\}\s*\)/);
  if (!objMatch) return params;
  const inner = objMatch[1];
  const keyRe = /(\w+)\s*:\s*Joi\.(number|string|boolean|array|object)(\.\w+(?:\([^)]*\))?)*(\.required\(\)|\.optional\(\))?/g;
  let m: RegExpExecArray | null;
  while ((m = keyRe.exec(inner)) !== null) {
    const required = m[4] ? m[4].includes('required') : undefined;
    params.push({ name: m[1], type: m[2], required });
  }
  return params;
}

const QUERY_PARAM_BLACKLIST = new Set(['json', 'length']); // common false positives from res.json, .length

// Parse req.query.X and known helpers (from, to, sort, all) for query params
function parseQueryParams(snippet: string): ParamInfo[] {
  const seen = new Set<string>();
  const params: ParamInfo[] = [];
  const add = (name: string, type = 'string', required?: boolean) => {
    if (seen.has(name) || QUERY_PARAM_BLACKLIST.has(name)) return;
    seen.add(name);
    params.push({ name, type, required });
  };
  const re = /req\.query\.(\w+)|req\.query\s+as\s+[^.]*\.(\w+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(snippet)) !== null) {
    const name = (m[1] || m[2]) ?? '';
    if (name) add(name);
  }
  if (snippet.includes('normalizeFromTo') || snippet.includes('parseDateRange')) {
    add('from', 'string (date or timestamp)');
    add('to', 'string (date or timestamp)');
  }
  if (snippet.includes('getProfileOrderBy') || snippet.includes('getHostProfileOrderBy')) add('sort');
  if (snippet.includes('parseFilledOnlyParam') || snippet.includes('getSquadCountsPerDayOrderBy')) {
    if (snippet.includes('parseFilledOnlyParam')) add('all', 'string (1/0 or true/false)');
    if (snippet.includes('getSquadCountsPerDayOrderBy')) add('sort', 'date|filled|total|unfilled');
  }
  if (snippet.includes('status') && snippet.includes('req.query')) add('status');
  if (snippet.includes('memberIds') && snippet.includes('req.query')) add('memberIds');
  if (snippet.includes('relicIds') && snippet.includes('req.query')) add('relicIds');
  if (snippet.includes('refinementIds') && snippet.includes('req.query')) add('refinementIds');
  if (snippet.includes('era') && snippet.includes('req.query')) add('era');
  if (snippet.includes('style') && snippet.includes('req.query')) add('style');
  if (snippet.includes('hostMemberId') && snippet.includes('req.query')) add('hostMemberId');
  if (snippet.includes('originatingServerId') && snippet.includes('req.query')) add('originatingServerId');
  if (snippet.includes('filled') && snippet.includes('req.query')) add('filled');
  return params;
}

// Parse res.json(...) for response shape
function parseResponseShape(snippet: string): string {
  const jsonMatch = snippet.match(/res\.(?:status\s*\(\s*\d+\s*\)\s*\.)?json\s*\(\s*(\w+)\s*\)/);
  if (jsonMatch) return jsonMatch[1] + ' (see handler for shape)';
  const objMatch = snippet.match(/res\.(?:status\s*\(\s*\d+\s*\)\s*\.)?json\s*\(\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\s*\}\)/);
  if (objMatch) {
    const inner = objMatch[1];
    const keys: string[] = [];
    const keyRe = /(\w+)\s*:/g;
    let m: RegExpExecArray | null;
    while ((m = keyRe.exec(inner)) !== null) keys.push(m[1]);
    if (keys.length) return '{ ' + keys.join(', ') + ' }';
  }
  if (snippet.includes('res.json(')) return '(see handler)';
  if (snippet.includes('res.send(')) return '(see handler)';
  if (snippet.includes('res.render(')) return 'HTML (pug view)';
  return '';
}

function getRoutesFromFileWithPath(content: string, sourceFile: string): RouteFromFile[] {
  const routes: RouteFromFile[] = [];
  const methods = ['get', 'post', 'put', 'delete', 'patch'];
  const seen = new Set<string>(); // method:routePath to avoid duplicates

  for (const method of methods) {
    // Only match JSDoc that ends with */ immediately before router.METHOD (so we don't capture code)
    const blockRe = new RegExp(
      `/\\*\\*([^*]*(?:\\*(?!\/)[^*]*)*)\\*/\\s*router\\.${method}\\s*\\(\\s*['"\`]([^'"\`]+)['"\`]`,
      'g'
    );
    let m: RegExpExecArray | null;
    while ((m = blockRe.exec(content)) !== null) {
      const description = m[1].replace(/\s*\*\s?/g, ' ').replace(/\n/g, ' ').trim();
      const routePath = m[2];
      const key = `${method}:${routePath}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const snippet = getHandlerSnippet(content, method, routePath);
      routes.push({
        method: method.toUpperCase(),
        routePath,
        pathParams: getPathParams(routePath),
        queryParams: parseQueryParams(snippet),
        bodyParams: parseBodyParams(snippet),
        responseShape: parseResponseShape(snippet),
        description,
        sourceFile,
      });
    }
  }
  for (const method of methods) {
    const simpleRe = new RegExp(
      `router\\.${method}\\s*\\(\\s*['"\`]([^'"\`]+)['"\`]`,
      'g'
    );
    let m: RegExpExecArray | null;
    while ((m = simpleRe.exec(content)) !== null) {
      const routePath = m[1];
      const key = `${method}:${routePath}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const snippet = getHandlerSnippet(content, method, routePath);
      routes.push({
        method: method.toUpperCase(),
        routePath,
        pathParams: getPathParams(routePath),
        queryParams: parseQueryParams(snippet),
        bodyParams: parseBodyParams(snippet),
        responseShape: parseResponseShape(snippet),
        description: '',
        sourceFile,
      });
    }
  }
  return routes;
}

function collectAllEndpoints(): EndpointInfo[] {
  const mounts = getMounts();
  const results: EndpointInfo[] = [];
  for (const { path: basePath, name } of mounts) {
    const filePath = path.join(SRC, 'routes', `${name}.ts`);
    if (!fs.existsSync(filePath)) continue;
    const content = fs.readFileSync(filePath, 'utf-8');
    const fileRoutes = getRoutesFromFileWithPath(content, `src/routes/${name}.ts`);
    for (const r of fileRoutes) {
      const fullPath = (basePath + (r.routePath === '/' ? '' : r.routePath)).replace(/\/+/g, '/');
      const { routePath: _rp, ...rest } = r;
      results.push({ ...rest, path: fullPath });
    }
  }
  return results;
}

function dedupeEndpoints(endpoints: EndpointInfo[]): EndpointInfo[] {
  const seen = new Set<string>();
  return endpoints.filter((e) => {
    const key = `${e.method} ${e.path}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function paramSummary(params: ParamInfo[]): string {
  if (!params.length) return '—';
  return params.map((p) => (p.required === false ? `${p.name}? (${p.type})` : `${p.name} (${p.type})`)).join(', ');
}

function toMarkdown(endpoints: EndpointInfo[]): string {
  const lines: string[] = [
    '# API Endpoints',
    '',
    'Generated by `npx ts-node scripts/list-endpoints.ts`',
    '',
    '| Method | Path | Path params | Query | Body | Response | Description |',
    '|--------|------|-------------|-------|-----|----------|-------------|',
  ];
  for (const e of endpoints) {
    const pathP = e.pathParams.length ? e.pathParams.join(', ') : '—';
    const queryP = e.queryParams.length ? paramSummary(e.queryParams) : '—';
    const bodyP = e.bodyParams.length ? paramSummary(e.bodyParams) : '—';
    const resp = (e.responseShape || '—').replace(/\|/g, '\\|');
    const desc = (e.description || '—').replace(/\|/g, '\\|').replace(/\n/g, ' ').slice(0, 80);
    lines.push(`| ${e.method} | \`${e.path}\` | ${pathP} | ${queryP} | ${bodyP} | ${resp} | ${desc} |`);
  }
  lines.push(
    '',
    '## Notes',
    '',
    '- **Path params** are in the URL path (e.g. `:id`).',
    '- **Query** params are sent as `?key=value`.',
    '- **Body** is for POST/PUT; shape is inferred from Joi schema in the handler.',
    '- **Response** is the general structure of `res.json(...)` (see handler for full shape).',
  );
  return lines.join('\n');
}

function main(): void {
  let endpoints = dedupeEndpoints(collectAllEndpoints());
  endpoints = endpoints.sort((a, b) => {
    const pathCmp = a.path.localeCompare(b.path);
    if (pathCmp !== 0) return pathCmp;
    return a.method.localeCompare(b.method);
  });
  const outDir = __dirname;
  const jsonPath = path.join(outDir, 'endpoints.json');
  const mdPath = path.join(outDir, 'endpoints.md');
  fs.writeFileSync(jsonPath, JSON.stringify({ endpoints }, null, 2), 'utf-8');
  fs.writeFileSync(mdPath, toMarkdown(endpoints), 'utf-8');
  console.log(`Wrote ${endpoints.length} endpoints to ${jsonPath} and ${mdPath}`);
}

main();
