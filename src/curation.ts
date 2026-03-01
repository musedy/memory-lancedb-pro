import type { MemoryStore } from "./store.js";

export interface CurationConfig {
  enabled: boolean;
  decayHalfLifeDays: number;
  pruneThreshold: number;
  maxDeletesPerRun: number;
  schedule: string;
}

export interface CurationRunResult {
  enabled: boolean;
  schedule: string;
  plannedDeletes: number;
  appliedDeletes: number;
  dryRun: boolean;
}

interface CurationEngineDeps {
  store: MemoryStore;
  logger?: {
    info?: (msg: string) => void;
    debug?: (msg: string) => void;
    warn?: (msg: string) => void;
  };
  config: CurationConfig;
}

export const DEFAULT_CURATION_CONFIG: CurationConfig = {
  enabled: false,
  decayHalfLifeDays: 60,
  pruneThreshold: 0.2,
  maxDeletesPerRun: 100,
  schedule: "0 3 * * *",
};

export class MemoryCurationEngine {
  private readonly store: MemoryStore;
  private readonly logger: CurationEngineDeps["logger"];
  private readonly config: CurationConfig;

  constructor(deps: CurationEngineDeps) {
    this.store = deps.store;
    this.logger = deps.logger;
    this.config = deps.config;
  }

  start(): void {
    if (!this.config.enabled) {
      return;
    }

    // Phase 3 scaffold only: no mutation/delete path is executed yet.
    this.logger?.info?.(
      `memory-lancedb-pro: curation scaffold enabled (schedule=${this.config.schedule}, maxDeletesPerRun=${this.config.maxDeletesPerRun})`
    );
  }

  stop(): void {
    // Placeholder for future scheduler/timer cleanup.
  }

  async runOnce(trigger = "manual"): Promise<CurationRunResult> {
    if (!this.config.enabled) {
      return {
        enabled: false,
        schedule: this.config.schedule,
        plannedDeletes: 0,
        appliedDeletes: 0,
        dryRun: true,
      };
    }

    // Touch dependency so scaffold wiring stays type-safe while executor is pending.
    void this.store;

    this.logger?.debug?.(`memory-lancedb-pro: curation scaffold run requested (trigger=${trigger})`);

    return {
      enabled: true,
      schedule: this.config.schedule,
      plannedDeletes: 0,
      appliedDeletes: 0,
      dryRun: true,
    };
  }

  getConfig(): CurationConfig {
    return { ...this.config };
  }
}

export function createMemoryCurationEngine(deps: CurationEngineDeps): MemoryCurationEngine {
  return new MemoryCurationEngine(deps);
}
