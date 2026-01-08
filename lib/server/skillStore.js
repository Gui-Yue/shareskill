import fs from "fs/promises";
import path from "path";
import initSqlJs from "sql.js";
import { parseArrayValue, pickCategory } from "../utils";

let cachedDb = null;
let cachedTable = null;
let cachedSql = null;
let loadingPromise = null;
let cachedMeta = {
  source: "",
  etag: "",
  lastModified: "",
  lastChecked: 0,
  mtimeMs: 0
};

const DEFAULT_DB_PATH = path.join(process.cwd(), "public", "skill.db");
const REVALIDATE_MS = Number(process.env.SKILL_DB_REVALIDATE_MS) || 4 * 60 * 60 * 1000;

function getDbSource() {
  const remoteUrl =
    process.env.SKILL_DB_URL || process.env.NEXT_PUBLIC_SKILL_DB_URL || "";
  if (remoteUrl && /^https?:\/\//i.test(remoteUrl)) {
    return { type: "remote", key: remoteUrl, url: remoteUrl };
  }
  return { type: "local", key: DEFAULT_DB_PATH, path: DEFAULT_DB_PATH };
}

async function getSql() {
  if (cachedSql) {
    return cachedSql;
  }
  cachedSql = await initSqlJs({
    locateFile: (file) => path.join(process.cwd(), "public", file)
  });
  return cachedSql;
}

function setCachedDb(db, table, meta) {
  if (cachedDb && typeof cachedDb.close === "function") {
    cachedDb.close();
  }
  cachedDb = db;
  cachedTable = table;
  cachedMeta = {
    ...cachedMeta,
    ...meta
  };
}

function selectTableName(db) {
  const result = db.exec(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
  );
  const names = result[0]?.values?.map((row) => row[0]) || [];
  if (names.includes("skills")) {
    return "skills";
  }
  if (names.includes("skill")) {
    return "skill";
  }
  return names[0] || null;
}

function rowsFromStatement(statement) {
  const rows = [];
  while (statement.step()) {
    rows.push(statement.getAsObject());
  }
  return rows;
}

function normalizeSkill(row, index) {
  const tags = parseArrayValue(row.tags);
  const tagsEn = parseArrayValue(row.tags_en || row.tagsEn);
  const categories = parseArrayValue(row.categories);
  const skillName = row.skill_name || row.skillName || row.name || "";
  const descriptionZh =
    row.description_zh || row.descriptionZh || row.description || "";
  const descriptionEn = row.description_en || row.descriptionEn || "";
  const useCaseEn = row.use_case_en || row.useCaseEn || "";
  const identifier = row.id !== undefined && row.id !== null
    ? String(row.id)
    : skillName
      ? encodeURIComponent(skillName)
      : String(index + 1);

  return {
    id: row.id ?? identifier,
    identifier,
    skill_name: skillName,
    fromRepo: row.fromRepo || row.from_repo || "",
    skillPath: row.skillPath || row.skill_path || "",
    repostars: row.repostars || row.repoStars || row.stars || 0,
    tagline: row.tagline || "",
    tags,
    tags_en: tagsEn,
    categories,
    category: categories[0] || pickCategory(row.categories),
    description: descriptionZh,
    description_zh: descriptionZh,
    description_en: descriptionEn,
    use_case: row.use_case || row.useCase || "",
    use_case_en: useCaseEn,
    download_url: row.download_url || row.downloadUrl || "",
    skill_md_content: row.skill_md_content || row.skillMdContent || "",
    skill_md_content_translation:
      row.skill_md_content_translation || row.skillMdContentTranslation || "",
    file_tree: row.file_tree || row.fileTree || "",
    how_to_install: row.how_to_install || row.howToInstall || "",
    created_at: row.created_at || row.createdAt || "",
    updated_at: row.updated_at || row.updatedAt || ""
  };
}

async function buildDbFromBuffer(buffer, meta) {
  const SQL = await getSql();
  const db = new SQL.Database(new Uint8Array(buffer));
  const table = selectTableName(db);
  if (!table) {
    throw new Error("No skills table found");
  }
  setCachedDb(db, table, meta);
  return cachedDb;
}

async function loadLocalDb(dbPath) {
  const stats = await fs.stat(dbPath);
  if (
    cachedDb &&
    cachedMeta.source === dbPath &&
    cachedMeta.mtimeMs === stats.mtimeMs
  ) {
    cachedMeta.lastChecked = Date.now();
    return cachedDb;
  }
  const buffer = await fs.readFile(dbPath);
  return buildDbFromBuffer(buffer, {
    source: dbPath,
    lastChecked: Date.now(),
    mtimeMs: stats.mtimeMs,
    etag: "",
    lastModified: ""
  });
}

async function revalidateRemoteDb(dbUrl) {
  const headers = {};
  if (cachedMeta.etag) {
    headers["If-None-Match"] = cachedMeta.etag;
  }
  if (cachedMeta.lastModified) {
    headers["If-Modified-Since"] = cachedMeta.lastModified;
  }

  let headResponse = null;
  try {
    headResponse = await fetch(dbUrl, {
      method: "HEAD",
      headers,
      cache: "no-store"
    });
  } catch (error) {
    headResponse = null;
  }

  if (headResponse?.status === 304) {
    return {
      notModified: true,
      meta: {
        etag: cachedMeta.etag,
        lastModified: cachedMeta.lastModified
      }
    };
  }

  if (headResponse?.ok) {
    const etag = headResponse.headers.get("etag") || "";
    const lastModified = headResponse.headers.get("last-modified") || "";
    if (
      (etag && etag === cachedMeta.etag) ||
      (lastModified && lastModified === cachedMeta.lastModified)
    ) {
      return { notModified: true, meta: { etag, lastModified } };
    }
  }

  const response = await fetch(dbUrl, {
    method: "GET",
    headers,
    cache: "no-store"
  });
  if (response.status === 304) {
    return {
      notModified: true,
      meta: {
        etag: cachedMeta.etag,
        lastModified: cachedMeta.lastModified
      }
    };
  }
  if (!response.ok) {
    throw new Error(`Failed to fetch remote skill db (${response.status})`);
  }
  const buffer = await response.arrayBuffer();
  const etag = response.headers.get("etag") || "";
  const lastModified = response.headers.get("last-modified") || "";
  return { buffer, meta: { etag, lastModified } };
}

async function loadRemoteDb(dbUrl) {
  const now = Date.now();
  if (cachedDb && cachedMeta.source === dbUrl) {
    try {
      const result = await revalidateRemoteDb(dbUrl);
      if (result.notModified) {
        cachedMeta = {
          ...cachedMeta,
          ...result.meta,
          lastChecked: now
        };
        return cachedDb;
      }
      if (result.buffer) {
        return buildDbFromBuffer(result.buffer, {
          source: dbUrl,
          lastChecked: now,
          mtimeMs: 0,
          ...result.meta
        });
      }
    } catch (error) {
      cachedMeta.lastChecked = now;
      if (cachedDb) {
        return cachedDb;
      }
      throw error;
    }
  }

  const response = await fetch(dbUrl, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch remote skill db (${response.status})`);
  }
  const buffer = await response.arrayBuffer();
  const etag = response.headers.get("etag") || "";
  const lastModified = response.headers.get("last-modified") || "";
  return buildDbFromBuffer(buffer, {
    source: dbUrl,
    lastChecked: now,
    mtimeMs: 0,
    etag,
    lastModified
  });
}

async function getDb() {
  const source = getDbSource();
  const needsCheck =
    !cachedDb ||
    cachedMeta.source !== source.key ||
    Date.now() - cachedMeta.lastChecked > REVALIDATE_MS;

  if (!needsCheck && cachedDb) {
    return cachedDb;
  }

  if (!loadingPromise) {
    loadingPromise = (async () => {
      try {
        if (source.type === "remote") {
          const db = await loadRemoteDb(source.url);
          loadingPromise = null;
          return db;
        }
        const db = await loadLocalDb(source.path);
        loadingPromise = null;
        return db;
      } catch (error) {
        loadingPromise = null;
        throw error;
      }
    })();
  }
  return loadingPromise;
}

function buildWhereClause({ q, category }) {
  const conditions = [];
  const params = [];
  if (category) {
    conditions.push("categories LIKE ?");
    params.push(`%${category}%`);
  }
  if (q) {
    const like = `%${q}%`;
    conditions.push(
      [
        "skill_name LIKE ?",
        "tagline LIKE ?",
        "description LIKE ?",
        "description_zh LIKE ?",
        "description_en LIKE ?",
        "use_case LIKE ?",
        "use_case_en LIKE ?",
        "tags LIKE ?",
        "tags_en LIKE ?"
      ].join(" OR ")
    );
    params.push(like, like, like, like, like, like, like, like, like);
  }
  const clause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  return { clause, params };
}

function buildOrderClause(sort) {
  if (sort === "stars") {
    return "ORDER BY repostars DESC";
  }
  if (sort === "oldest") {
    return "ORDER BY updated_at ASC";
  }
  return "ORDER BY updated_at DESC";
}

export async function listSkills({
  q = "",
  category = "",
  page = 1,
  pageSize = 16,
  sort = "latest"
} = {}) {
  const db = await getDb();
  const table = cachedTable || selectTableName(db);
  if (!table) {
    return { items: [], total: 0, page, pageSize };
  }

  const { clause, params } = buildWhereClause({ q, category });
  const orderClause = buildOrderClause(sort);
  const offset = Math.max(0, (page - 1) * pageSize);

  const countStmt = db.prepare(
    `SELECT COUNT(*) as count FROM "${table}" ${clause}`
  );
  countStmt.bind(params);
  const countRow = countStmt.step() ? countStmt.getAsObject() : { count: 0 };
  countStmt.free();
  const total = Number(countRow.count) || 0;

  const stmt = db.prepare(
    `SELECT id, skill_name, repostars, tagline, tags, tags_en, categories, updated_at
     FROM "${table}"
     ${clause}
     ${orderClause}
     LIMIT ? OFFSET ?`
  );
  stmt.bind([...params, pageSize, offset]);
  const rows = rowsFromStatement(stmt);
  stmt.free();

  const items = rows.map((row, index) => normalizeSkill(row, index));
  return { items, total, page, pageSize };
}

export async function getSkillByIdentifier(identifier) {
  const db = await getDb();
  const table = cachedTable || selectTableName(db);
  if (!table) {
    return null;
  }
  const decoded = decodeURIComponent(identifier || "");
  let stmt;
  if (/^\d+$/.test(decoded)) {
    stmt = db.prepare(`SELECT * FROM "${table}" WHERE id = ? LIMIT 1`);
    stmt.bind([Number(decoded)]);
  } else {
    stmt = db.prepare(
      `SELECT * FROM "${table}" WHERE skill_name = ? LIMIT 1`
    );
    stmt.bind([decoded]);
  }
  const rows = rowsFromStatement(stmt);
  stmt.free();
  if (!rows.length) {
    return null;
  }
  return normalizeSkill(rows[0], 0);
}

export async function getSkillSummary() {
  const db = await getDb();
  const table = cachedTable || selectTableName(db);
  if (!table) {
    return { total: 0, counts: {} };
  }
  const stmt = db.prepare(`SELECT categories FROM "${table}"`);
  const rows = rowsFromStatement(stmt);
  stmt.free();
  const counts = {};
  rows.forEach((row) => {
    const category = pickCategory(row.categories);
    counts[category] = (counts[category] || 0) + 1;
  });
  return { total: rows.length, counts };
}
