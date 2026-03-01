/**
 * LanceDB Storage Layer with Multi-Scope Support
 */

import type * as LanceDB from "@lancedb/lancedb";
import { randomUUID } from "node:crypto";

// ============================================================================
// Types
// ============================================================================

export interface MemoryEntry {
  id: string;
  text: string;
  vector: number[];
  category: "preference" | "fact" | "decision" | "entity" | "other";
  scope: string;
  importance: number;
  timestamp: number;
  memory_type?: string;
  access_count?: number;
  last_accessed_at?: number | null;
  reinforcement_score?: number;
  utility_score?: number;
  metadata?: string; // JSON string for extensible metadata
}

export interface MemorySearchResult {
  entry: MemoryEntry;
  score: number;
}

export interface MemoryWriteResult {
  action: "created" | "merged";
  entry: MemoryEntry;
  similarity?: number;
  mergedIntoId?: string;
}

export interface StoreConfig {
  dbPath: string;
  vectorDim: number;
}

interface MemoryLifecycleDefaults {
  memory_type: string;
  access_count: number;
  last_accessed_at: number | null;
  reinforcement_score: number;
  utility_score: number;
}

// ============================================================================
// LanceDB Dynamic Import
// ============================================================================

let lancedbImportPromise: Promise<typeof import("@lancedb/lancedb")> | null = null;

export const loadLanceDB = async (): Promise<typeof import("@lancedb/lancedb")> => {
  if (!lancedbImportPromise) {
    lancedbImportPromise = import("@lancedb/lancedb");
  }
  try {
    return await lancedbImportPromise;
  } catch (err) {
    throw new Error(`memory-lancedb-pro: failed to load LanceDB. ${String(err)}`, { cause: err });
  }
};

// ============================================================================
// Utility Functions
// ============================================================================

function clampInt(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function toFiniteNumber(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  return fallback;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
  return Math.max(0, Math.floor(toFiniteNumber(value, fallback)));
}

function toMemoryType(value: unknown): string {
  if (typeof value === "string" && value.trim().length > 0) {
    return value.trim();
  }
  return "fact";
}

function toNullableTimestamp(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return value;
  }
  return null;
}

function clamp01(value: number, fallback: number): number {
  if (!Number.isFinite(value)) {
    return Math.min(1, Math.max(0, fallback));
  }
  return Math.min(1, Math.max(0, value));
}

const MEMORY_LIFECYCLE_DEFAULTS: MemoryLifecycleDefaults = {
  memory_type: "fact",
  access_count: 0,
  last_accessed_at: null,
  reinforcement_score: 0,
  utility_score: 0,
};

// ============================================================================
// Memory Store
// ============================================================================

const TABLE_NAME = "memories";
const DEFAULT_STORE_DUPLICATE_THRESHOLD = 0.98;
const DEFAULT_IMPORT_DUPLICATE_THRESHOLD = 0.95;
type LanceRecord = Record<string, unknown>;

function toLanceRecord(entry: MemoryEntry): LanceRecord {
  return entry as unknown as LanceRecord;
}

type MemoryStoreInput = Omit<
  MemoryEntry,
  "id" | "timestamp" | keyof MemoryLifecycleDefaults
> & Partial<MemoryLifecycleDefaults>;

export class MemoryStore {
  private db: LanceDB.Connection | null = null;
  private table: LanceDB.Table | null = null;
  private initPromise: Promise<void> | null = null;
  private ftsIndexCreated = false;

  constructor(private readonly config: StoreConfig) {}

  get dbPath(): string {
    return this.config.dbPath;
  }

  private async ensureInitialized(): Promise<void> {
    if (this.table) {
      return;
    }
    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = this.doInitialize().catch((err) => {
      this.initPromise = null;
      throw err;
    });
    return this.initPromise;
  }

  private async doInitialize(): Promise<void> {
    const lancedb = await loadLanceDB();
    const db = await lancedb.connect(this.config.dbPath);
    let table: LanceDB.Table;

    // Idempotent table init: try openTable first, create only if missing,
    // and handle the race where tableNames() misses an existing table but
    // createTable then sees it (LanceDB eventual consistency).
    try {
      table = await db.openTable(TABLE_NAME);

      // Check if we need to add scope column for backward compatibility
      try {
        const sample = await table.query().limit(1).toArray();
        if (sample.length > 0 && !("scope" in sample[0])) {
          console.warn("Adding scope column for backward compatibility with existing data");
        }
      } catch (err) {
        console.warn("Could not check table schema:", err);
      }
    } catch (_openErr) {
      // Table doesn't exist yet — create it
      const schemaEntry: MemoryEntry = {
        id: "__schema__",
        text: "",
        vector: Array.from({ length: this.config.vectorDim }).fill(0) as number[],
        category: "other",
        scope: "global",
        importance: 0,
        timestamp: 0,
        memory_type: MEMORY_LIFECYCLE_DEFAULTS.memory_type,
        access_count: MEMORY_LIFECYCLE_DEFAULTS.access_count,
        last_accessed_at: MEMORY_LIFECYCLE_DEFAULTS.last_accessed_at,
        reinforcement_score: MEMORY_LIFECYCLE_DEFAULTS.reinforcement_score,
        utility_score: MEMORY_LIFECYCLE_DEFAULTS.utility_score,
        metadata: "{}",
      };

      try {
        table = await db.createTable(TABLE_NAME, [toLanceRecord(schemaEntry)]);
        await table.delete('id = "__schema__"');
      } catch (createErr) {
        // Race: another caller (or eventual consistency) created the table
        // between our failed openTable and this createTable — just open it.
        if (String(createErr).includes("already exists")) {
          table = await db.openTable(TABLE_NAME);
        } else {
          throw createErr;
        }
      }
    }

    await this.ensureLifecycleColumns(table);

    // Validate vector dimensions
    // Note: LanceDB returns Arrow Vector objects, not plain JS arrays.
    // Array.isArray() returns false for Arrow Vectors, so use .length instead.
    const sample = await table.query().limit(1).toArray();
    if (sample.length > 0 && sample[0]?.vector?.length) {
      const existingDim = sample[0].vector.length;
      if (existingDim !== this.config.vectorDim) {
        throw new Error(
          `Vector dimension mismatch: table=${existingDim}, config=${this.config.vectorDim}. Create a new table/dbPath or set matching embedding.dimensions.`
        );
      }
    }

    // Create FTS index for BM25 search (graceful fallback if unavailable)
    try {
      await this.createFtsIndex(table);
      this.ftsIndexCreated = true;
    } catch (err) {
      console.warn("Failed to create FTS index, falling back to vector-only search:", err);
      this.ftsIndexCreated = false;
    }

    this.db = db;
    this.table = table;
  }

  private async createFtsIndex(table: LanceDB.Table): Promise<void> {
    try {
      // Check if FTS index already exists
      const indices = await table.listIndices();
      const hasFtsIndex = indices?.some((idx: any) =>
        idx.indexType === "FTS" || idx.columns?.includes("text")
      );

      if (!hasFtsIndex) {
        // LanceDB @lancedb/lancedb >=0.26: use Index.fts() config
        const lancedb = await loadLanceDB();
        await table.createIndex("text", {
          config: (lancedb as any).Index.fts(),
        });
      }
    } catch (err) {
      throw new Error(`FTS index creation failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  private async getTableColumnNames(table: LanceDB.Table): Promise<Set<string>> {
    try {
      const schema = await table.schema();
      const fields = (schema as any)?.fields;
      if (Array.isArray(fields) && fields.length > 0) {
        return new Set(
          fields
            .map((field: any) => (typeof field?.name === "string" ? field.name : ""))
            .filter((name: string) => name.length > 0)
        );
      }
    } catch {
      // Fall back to row inspection below.
    }

    const sample = await table.query().limit(1).toArray();
    if (sample.length === 0) return new Set();
    return new Set(Object.keys(sample[0] as Record<string, unknown>));
  }

  private async ensureLifecycleColumns(table: LanceDB.Table): Promise<void> {
    const requiredColumns = [
      { name: "memory_type", valueSql: "'fact'" },
      { name: "access_count", valueSql: "0" },
      { name: "last_accessed_at", valueSql: "NULL" },
      { name: "reinforcement_score", valueSql: "0.0" },
      { name: "utility_score", valueSql: "0.0" },
    ];

    const existingColumns = await this.getTableColumnNames(table);
    const missingColumns = requiredColumns.filter((col) => !existingColumns.has(col.name));
    if (missingColumns.length === 0) return;

    try {
      await table.addColumns(missingColumns);
      console.log(`memory-lancedb-pro: added lifecycle columns: ${missingColumns.map(c => c.name).join(", ")}`);
    } catch (err) {
      console.warn(`memory-lancedb-pro: lifecycle column migration skipped: ${String(err)}`);
    }
  }

  private normalizeEntry(row: any, options?: { includeVector?: boolean }): MemoryEntry {
    const includeVector = options?.includeVector !== false;
    const rawVector = row.vector as Iterable<number> | undefined;
    const vector = includeVector ? Array.from(rawVector || []) : [];

    return {
      id: row.id as string,
      text: row.text as string,
      vector,
      category: row.category as MemoryEntry["category"],
      scope: (row.scope as string | undefined) ?? "global",
      importance: toFiniteNumber(row.importance, 0.7),
      timestamp: toFiniteNumber(row.timestamp, Date.now()),
      memory_type: toMemoryType(row.memory_type),
      access_count: toNonNegativeInt(row.access_count, MEMORY_LIFECYCLE_DEFAULTS.access_count),
      last_accessed_at: toNullableTimestamp(row.last_accessed_at),
      reinforcement_score: toFiniteNumber(
        row.reinforcement_score,
        MEMORY_LIFECYCLE_DEFAULTS.reinforcement_score
      ),
      utility_score: toFiniteNumber(row.utility_score, MEMORY_LIFECYCLE_DEFAULTS.utility_score),
      metadata: (row.metadata as string) || "{}",
    };
  }

  private async replaceEntry(entry: MemoryEntry): Promise<void> {
    const safeId = escapeSqlLiteral(entry.id);
    await this.table!.delete(`id = '${safeId}'`);
    await this.table!.add([toLanceRecord(entry)]);
  }

  private mergeDuplicate(existing: MemoryEntry, incoming: Partial<MemoryEntry>, touchedAt: number): MemoryEntry {
    const incomingAccessBoost = Math.max(1, toNonNegativeInt(incoming.access_count, 1));
    const nextAccessCount = Math.max(0, (existing.access_count ?? 0) + incomingAccessBoost);
    const formulaReinforcement = 1 - Math.exp(-nextAccessCount / 5);
    const formulaUtility = 1 - Math.exp(-nextAccessCount / 8);
    const existingImportance = clamp01(existing.importance ?? 0.7, 0.7);
    const incomingImportance = clamp01(toFiniteNumber(incoming.importance, existingImportance), existingImportance);
    const incomingText = typeof incoming.text === "string" ? incoming.text.trim() : "";
    const existingText = typeof existing.text === "string" ? existing.text : "";
    const incomingCategory = incoming.category as MemoryEntry["category"] | undefined;

    const nextReinforcement = clamp01(
      Math.max(
        existing.reinforcement_score ?? 0,
        toFiniteNumber(incoming.reinforcement_score, 0),
        formulaReinforcement
      ),
      existing.reinforcement_score ?? 0
    );
    const nextUtility = clamp01(
      Math.max(
        existing.utility_score ?? 0,
        toFiniteNumber(incoming.utility_score, 0),
        formulaUtility
      ),
      existing.utility_score ?? 0
    );

    return {
      ...existing,
      text: incomingText.length > existingText.length ? incomingText : existingText,
      vector: Array.isArray(incoming.vector) && incoming.vector.length === this.config.vectorDim
        ? incoming.vector
        : existing.vector,
      category:
        existing.category === "other" &&
        incomingCategory &&
        incomingCategory !== "other"
          ? incomingCategory
          : existing.category,
      importance: Math.max(existingImportance, incomingImportance),
      timestamp: Math.min(
        existing.timestamp ?? touchedAt,
        toFiniteNumber(incoming.timestamp, existing.timestamp ?? touchedAt)
      ),
      memory_type: toMemoryType(incoming.memory_type ?? existing.memory_type),
      access_count: nextAccessCount,
      last_accessed_at: touchedAt,
      reinforcement_score: nextReinforcement,
      utility_score: nextUtility,
      metadata:
        (typeof existing.metadata === "string" && existing.metadata.trim().length > 0
          ? existing.metadata
          : null) ??
        (typeof incoming.metadata === "string" && incoming.metadata.trim().length > 0
          ? incoming.metadata
          : "{}"),
    };
  }

  async store(entry: MemoryStoreInput): Promise<MemoryEntry> {
    await this.ensureInitialized();

    const fullEntry: MemoryEntry = {
      ...entry,
      id: randomUUID(),
      timestamp: Date.now(),
      memory_type: toMemoryType(entry.memory_type),
      access_count: toNonNegativeInt(entry.access_count, MEMORY_LIFECYCLE_DEFAULTS.access_count),
      last_accessed_at: toNullableTimestamp(entry.last_accessed_at),
      reinforcement_score: toFiniteNumber(
        entry.reinforcement_score,
        MEMORY_LIFECYCLE_DEFAULTS.reinforcement_score
      ),
      utility_score: toFiniteNumber(entry.utility_score, MEMORY_LIFECYCLE_DEFAULTS.utility_score),
      metadata: entry.metadata || "{}",
    };

    await this.table!.add([toLanceRecord(fullEntry)]);
    return fullEntry;
  }

  async storeOrMerge(
    entry: MemoryStoreInput,
    options?: { duplicateThreshold?: number }
  ): Promise<MemoryWriteResult> {
    await this.ensureInitialized();

    const vector = entry.vector || [];
    if (!Array.isArray(vector) || vector.length !== this.config.vectorDim) {
      throw new Error(
        `Vector dimension mismatch: expected ${this.config.vectorDim}, got ${Array.isArray(vector) ? vector.length : "non-array"}`
      );
    }

    const scope = typeof entry.scope === "string" && entry.scope.trim().length > 0 ? entry.scope : "global";
    const duplicateThreshold = clamp01(
      toFiniteNumber(options?.duplicateThreshold, DEFAULT_STORE_DUPLICATE_THRESHOLD),
      DEFAULT_STORE_DUPLICATE_THRESHOLD
    );

    const existing = await this.vectorSearch(vector, 1, 0.1, [scope]);
    if (existing.length > 0 && existing[0].score >= duplicateThreshold) {
      const merged = this.mergeDuplicate(existing[0].entry, { ...entry, scope }, Date.now());
      await this.replaceEntry(merged);
      return {
        action: "merged",
        entry: merged,
        similarity: existing[0].score,
        mergedIntoId: existing[0].entry.id,
      };
    }

    const created = await this.store({ ...entry, scope });
    return { action: "created", entry: created };
  }

  /**
   * Import a pre-built entry while preserving its id/timestamp.
   * Used for re-embedding / migration / A/B testing across embedding models.
   * Intentionally separate from `store()` to keep normal writes simple.
   */
  async importEntry(entry: MemoryEntry): Promise<MemoryEntry> {
    await this.ensureInitialized();

    if (!entry.id || typeof entry.id !== "string") {
      throw new Error("importEntry requires a stable id");
    }

    const vector = entry.vector || [];
    if (!Array.isArray(vector) || vector.length !== this.config.vectorDim) {
      throw new Error(
        `Vector dimension mismatch: expected ${this.config.vectorDim}, got ${Array.isArray(vector) ? vector.length : 'non-array'}`
      );
    }

    const full: MemoryEntry = {
      ...entry,
      scope: entry.scope || "global",
      importance: Number.isFinite(entry.importance) ? entry.importance : 0.7,
      timestamp: Number.isFinite(entry.timestamp) ? entry.timestamp : Date.now(),
      memory_type: toMemoryType(entry.memory_type),
      access_count: toNonNegativeInt(entry.access_count, MEMORY_LIFECYCLE_DEFAULTS.access_count),
      last_accessed_at: toNullableTimestamp(entry.last_accessed_at),
      reinforcement_score: toFiniteNumber(
        entry.reinforcement_score,
        MEMORY_LIFECYCLE_DEFAULTS.reinforcement_score
      ),
      utility_score: toFiniteNumber(entry.utility_score, MEMORY_LIFECYCLE_DEFAULTS.utility_score),
      metadata: entry.metadata || "{}",
    };

    const safeId = escapeSqlLiteral(full.id);
    const sameId = await this.table!.query().where(`id = '${safeId}'`).limit(1).toArray();
    if (sameId.length > 0) {
      await this.replaceEntry(full);
      return full;
    }

    const existing = await this.vectorSearch(full.vector, 1, 0.1, [full.scope]);
    if (
      existing.length > 0 &&
      existing[0].score >= DEFAULT_IMPORT_DUPLICATE_THRESHOLD &&
      existing[0].entry.id !== full.id
    ) {
      const merged = this.mergeDuplicate(existing[0].entry, full, Date.now());
      await this.replaceEntry(merged);
      return merged;
    }

    await this.table!.add([toLanceRecord(full)]);
    return full;
  }

  async hasId(id: string): Promise<boolean> {
    await this.ensureInitialized();
    const safeId = escapeSqlLiteral(id);
    const res = await this.table!.query().select(["id"]).where(`id = '${safeId}'`).limit(1).toArray();
    return res.length > 0;
  }

  async vectorSearch(vector: number[], limit = 5, minScore = 0.3, scopeFilter?: string[]): Promise<MemorySearchResult[]> {
    await this.ensureInitialized();

    const safeLimit = clampInt(limit, 1, 20);
    const fetchLimit = Math.min(safeLimit * 10, 200); // Over-fetch for scope filtering

    let query = this.table!.vectorSearch(vector).limit(fetchLimit);

    // Apply scope filter if provided
    if (scopeFilter && scopeFilter.length > 0) {
      const scopeConditions = scopeFilter
        .map(scope => `scope = '${escapeSqlLiteral(scope)}'`)
        .join(" OR ");
      query = query.where(`(${scopeConditions}) OR scope IS NULL`); // NULL for backward compatibility
    }

    const results = await query.toArray();
    const mapped: MemorySearchResult[] = [];

    for (const row of results) {
      const distance = row._distance ?? 0;
      const score = 1 / (1 + distance);

      if (score < minScore) continue;

      const rowScope = (row.scope as string | undefined) ?? "global";

      // Double-check scope filter in application layer
      if (scopeFilter && scopeFilter.length > 0 && !scopeFilter.includes(rowScope)) {
        continue;
      }

      mapped.push({
        entry: this.normalizeEntry({ ...row, scope: rowScope }),
        score,
      });

      if (mapped.length >= safeLimit) break;
    }

    return mapped;
  }

  async bm25Search(query: string, limit = 5, scopeFilter?: string[]): Promise<MemorySearchResult[]> {
    await this.ensureInitialized();

    if (!this.ftsIndexCreated) {
      return []; // Fallback to vector-only if FTS unavailable
    }

    const safeLimit = clampInt(limit, 1, 20);

    try {
      // Use FTS query type explicitly
      let searchQuery = this.table!.search(query, "fts").limit(safeLimit);

      // Apply scope filter if provided
      if (scopeFilter && scopeFilter.length > 0) {
        const scopeConditions = scopeFilter
          .map(scope => `scope = '${escapeSqlLiteral(scope)}'`)
          .join(" OR ");
        searchQuery = searchQuery.where(`(${scopeConditions}) OR scope IS NULL`);
      }

      const results = await searchQuery.toArray();
      const mapped: MemorySearchResult[] = [];

      for (const row of results) {
        const rowScope = (row.scope as string | undefined) ?? "global";

        // Double-check scope filter in application layer
        if (scopeFilter && scopeFilter.length > 0 && !scopeFilter.includes(rowScope)) {
          continue;
        }

        // LanceDB FTS _score is raw BM25 (unbounded). Normalize with sigmoid.
        const rawScore = typeof row._score === "number" ? row._score : 0;
        const normalizedScore = rawScore > 0 ? 1 / (1 + Math.exp(-rawScore / 5)) : 0.5;

        mapped.push({
          entry: this.normalizeEntry({ ...row, scope: rowScope }),
          score: normalizedScore,
        });
      }

      return mapped;
    } catch (err) {
      console.warn("BM25 search failed, falling back to empty results:", err);
      return [];
    }
  }

  async delete(id: string, scopeFilter?: string[]): Promise<boolean> {
    await this.ensureInitialized();

    // Support both full UUID and short prefix (8+ hex chars)
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const prefixRegex = /^[0-9a-f]{8,}$/i;
    const isFullId = uuidRegex.test(id);
    const isPrefix = !isFullId && prefixRegex.test(id);

    if (!isFullId && !isPrefix) {
      throw new Error(`Invalid memory ID format: ${id}`);
    }

    let candidates: any[];
    if (isFullId) {
      candidates = await this.table!.query().where(`id = '${id}'`).limit(1).toArray();
    } else {
      // Prefix match: fetch candidates and filter in app layer
      const all = await this.table!.query().select(["id", "scope"]).limit(1000).toArray();
      candidates = all.filter((r: any) => (r.id as string).startsWith(id));
      if (candidates.length > 1) {
        throw new Error(`Ambiguous prefix "${id}" matches ${candidates.length} memories. Use a longer prefix or full ID.`);
      }
    }
    if (candidates.length === 0) {
      return false;
    }

    const resolvedId = candidates[0].id as string;
    const rowScope = (candidates[0].scope as string | undefined) ?? "global";

    // Check scope permissions
    if (scopeFilter && scopeFilter.length > 0 && !scopeFilter.includes(rowScope)) {
      throw new Error(`Memory ${resolvedId} is outside accessible scopes`);
    }

    await this.table!.delete(`id = '${resolvedId}'`);
    return true;
  }

  async list(scopeFilter?: string[], category?: string, limit = 20, offset = 0): Promise<MemoryEntry[]> {
    await this.ensureInitialized();

    let query = this.table!.query();

    // Build where conditions
    const conditions: string[] = [];

    if (scopeFilter && scopeFilter.length > 0) {
      const scopeConditions = scopeFilter
        .map(scope => `scope = '${escapeSqlLiteral(scope)}'`)
        .join(" OR ");
      conditions.push(`((${scopeConditions}) OR scope IS NULL)`);
    }

    if (category) {
      conditions.push(`category = '${escapeSqlLiteral(category)}'`);
    }

    if (conditions.length > 0) {
      query = query.where(conditions.join(" AND "));
    }

    // Fetch all matching rows (no pre-limit) so app-layer sort is correct across full dataset
    const results = await query
      .select([
        "id",
        "text",
        "category",
        "scope",
        "importance",
        "timestamp",
        "memory_type",
        "access_count",
        "last_accessed_at",
        "reinforcement_score",
        "utility_score",
        "metadata",
      ])
      .toArray();

    return results
      .map((row): MemoryEntry => this.normalizeEntry(row, { includeVector: false }))
      .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
      .slice(offset, offset + limit);
  }

  async stats(scopeFilter?: string[]): Promise<{
    totalCount: number;
    scopeCounts: Record<string, number>;
    categoryCounts: Record<string, number>
  }> {
    await this.ensureInitialized();

    let query = this.table!.query();

    if (scopeFilter && scopeFilter.length > 0) {
      const scopeConditions = scopeFilter
        .map(scope => `scope = '${escapeSqlLiteral(scope)}'`)
        .join(" OR ");
      query = query.where(`((${scopeConditions}) OR scope IS NULL)`);
    }

    const results = await query.select(["scope", "category"]).toArray();

    const scopeCounts: Record<string, number> = {};
    const categoryCounts: Record<string, number> = {};

    for (const row of results) {
      const scope = (row.scope as string | undefined) ?? "global";
      const category = row.category as string;

      scopeCounts[scope] = (scopeCounts[scope] || 0) + 1;
      categoryCounts[category] = (categoryCounts[category] || 0) + 1;
    }

    return {
      totalCount: results.length,
      scopeCounts,
      categoryCounts,
    };
  }

  async update(
    id: string,
    updates: { text?: string; vector?: number[]; importance?: number; category?: MemoryEntry["category"]; metadata?: string },
    scopeFilter?: string[]
  ): Promise<MemoryEntry | null> {
    await this.ensureInitialized();

    // Support both full UUID and short prefix (8+ hex chars), same as delete()
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const prefixRegex = /^[0-9a-f]{8,}$/i;
    const isFullId = uuidRegex.test(id);
    const isPrefix = !isFullId && prefixRegex.test(id);

    if (!isFullId && !isPrefix) {
      throw new Error(`Invalid memory ID format: ${id}`);
    }

    let rows: any[];
    if (isFullId) {
      const safeId = escapeSqlLiteral(id);
      rows = await this.table!.query().where(`id = '${safeId}'`).limit(1).toArray();
    } else {
      // Prefix match
      const all = await this.table!
        .query()
        .select([
          "id",
          "text",
          "vector",
          "category",
          "scope",
          "importance",
          "timestamp",
          "memory_type",
          "access_count",
          "last_accessed_at",
          "reinforcement_score",
          "utility_score",
          "metadata",
        ])
        .limit(1000)
        .toArray();
      rows = all.filter((r: any) => (r.id as string).startsWith(id));
      if (rows.length > 1) {
        throw new Error(`Ambiguous prefix "${id}" matches ${rows.length} memories. Use a longer prefix or full ID.`);
      }
    }

    if (rows.length === 0) return null;

    const row = rows[0];
    const rowScope = (row.scope as string | undefined) ?? "global";

    // Check scope permissions
    if (scopeFilter && scopeFilter.length > 0 && !scopeFilter.includes(rowScope)) {
      throw new Error(`Memory ${id} is outside accessible scopes`);
    }

    // Build updated entry, preserving original timestamp
    const updated: MemoryEntry = {
      id: row.id as string,
      text: updates.text ?? (row.text as string),
      vector: updates.vector ?? (Array.from(row.vector as Iterable<number>)),
      category: updates.category ?? (row.category as MemoryEntry["category"]),
      scope: rowScope,
      importance: updates.importance ?? (row.importance as number),
      timestamp: row.timestamp as number, // preserve original
      memory_type: toMemoryType(row.memory_type),
      access_count: toNonNegativeInt(row.access_count, MEMORY_LIFECYCLE_DEFAULTS.access_count),
      last_accessed_at: toNullableTimestamp(row.last_accessed_at),
      reinforcement_score: toFiniteNumber(
        row.reinforcement_score,
        MEMORY_LIFECYCLE_DEFAULTS.reinforcement_score
      ),
      utility_score: toFiniteNumber(row.utility_score, MEMORY_LIFECYCLE_DEFAULTS.utility_score),
      metadata: updates.metadata ?? ((row.metadata as string) || "{}"),
    };

    // LanceDB doesn't support in-place update; delete + re-add
    await this.replaceEntry(updated);

    return updated;
  }

  async bulkDelete(scopeFilter: string[], beforeTimestamp?: number): Promise<number> {
    await this.ensureInitialized();

    const conditions: string[] = [];

    if (scopeFilter.length > 0) {
      const scopeConditions = scopeFilter
        .map(scope => `scope = '${escapeSqlLiteral(scope)}'`)
        .join(" OR ");
      conditions.push(`(${scopeConditions})`);
    }

    if (beforeTimestamp) {
      conditions.push(`timestamp < ${beforeTimestamp}`);
    }

    if (conditions.length === 0) {
      throw new Error("Bulk delete requires at least scope or timestamp filter for safety");
    }

    const whereClause = conditions.join(" AND ");

    // Count first
    const countResults = await this.table!.query().where(whereClause).toArray();
    const deleteCount = countResults.length;

    // Then delete
    if (deleteCount > 0) {
      await this.table!.delete(whereClause);
    }

    return deleteCount;
  }

  async markAccessed(ids: string[], scopeFilter?: string[]): Promise<number> {
    await this.ensureInitialized();

    const uniqueIds = Array.from(
      new Set(
        ids
          .filter((id): id is string => typeof id === "string")
          .map((id) => id.trim())
          .filter((id) => id.length > 0)
      )
    );
    if (uniqueIds.length === 0) {
      return 0;
    }

    const touchedAt = Date.now();
    let updatedCount = 0;

    for (const id of uniqueIds) {
      const safeId = escapeSqlLiteral(id);
      const rows = await this.table!.query().where(`id = '${safeId}'`).limit(1).toArray();
      if (rows.length === 0) {
        continue;
      }

      const row = rows[0];
      const rowScope = (row.scope as string | undefined) ?? "global";

      if (scopeFilter && scopeFilter.length > 0 && !scopeFilter.includes(rowScope)) {
        continue;
      }

      const existing = this.normalizeEntry({ ...row, scope: rowScope });
      const nextAccessCount = Math.max(0, (existing.access_count ?? 0) + 1);
      const nextReinforcement = clamp01(
        Math.max(existing.reinforcement_score ?? 0, 1 - Math.exp(-nextAccessCount / 5)),
        existing.reinforcement_score ?? 0
      );
      const nextUtility = clamp01(
        Math.max(existing.utility_score ?? 0, 1 - Math.exp(-nextAccessCount / 8)),
        existing.utility_score ?? 0
      );

      const updated: MemoryEntry = {
        ...existing,
        access_count: nextAccessCount,
        last_accessed_at: touchedAt,
        reinforcement_score: nextReinforcement,
        utility_score: nextUtility,
      };

      // LanceDB has no in-place partial update; emulate with delete + add.
      await this.replaceEntry(updated);
      updatedCount++;
    }

    return updatedCount;
  }

  get hasFtsSupport(): boolean {
    return this.ftsIndexCreated;
  }
}
