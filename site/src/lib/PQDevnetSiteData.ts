import yaml from 'js-yaml';
import fs from 'node:fs';
import path from 'node:path';

export interface PQDevnetNotebookConfig {
    id: string;
    title: string;
    description?: string;
    source: string;
    order: number;
    icon?: string;
}

export interface PQDevnetNotebookData {
    html_path: string;
    rendered_at?: string;
    notebook_hash?: string;
    data_hash?: string;
}

export interface PQDevnetManifest {
    latest_devnet?: string;
    devnets?: Record<string, Record<string, PQDevnetNotebookData>>;
    updated_at?: string;
}

export interface PQDevnetPipelineConfig {
    version?: string;
    notebooks: PQDevnetNotebookConfig[];
    settings?: {
        data_dir?: string;
        rendered_dir?: string;
        prometheus_url?: string;
    };
}

/**
 * Data access for PQ Devnet notebooks.
 * Separate from SiteData to avoid conflicts with upstream.
 */
export class PQDevnetSiteData {
    private readonly manifest: PQDevnetManifest;
    private readonly config: PQDevnetPipelineConfig;

    private constructor(manifest: PQDevnetManifest, config: PQDevnetPipelineConfig) {
        this.manifest = manifest;
        this.config = config;
    }

    /**
     * Load PQ Devnet site data from filesystem.
     */
    static load(): PQDevnetSiteData {
        const manifest = PQDevnetSiteData.loadManifest();
        const config = PQDevnetSiteData.loadConfig();
        return new PQDevnetSiteData(manifest, config);
    }

    private static loadManifest(): PQDevnetManifest {
        const manifestPath = path.join(process.cwd(), 'rendered', 'manifest.json');
        try {
            if (fs.existsSync(manifestPath)) {
                const content = fs.readFileSync(manifestPath, 'utf-8');
                return JSON.parse(content);
            }
        } catch (e) {
            console.error('Failed to load PQ Devnet manifest.json', e);
        }
        return { devnets: {} };
    }

    private static loadConfig(): PQDevnetPipelineConfig {
        const configPath = path.join(process.cwd(), '..', 'pqdevnet-pipeline.yaml');
        try {
            const configContent = fs.readFileSync(configPath, 'utf-8');
            return yaml.load(configContent) as PQDevnetPipelineConfig;
        } catch (e) {
            console.error('Failed to load pqdevnet-pipeline.yaml', e);
            return { notebooks: [] };
        }
    }

    /** The most recent devnet with rendered data */
    get latestDevnet(): string {
        return this.manifest.latest_devnet || '';
    }

    /** All available devnets, sorted by ID (newest last) */
    get availableDevnets(): string[] {
        if (!this.manifest.devnets) return [];
        return Object.keys(this.manifest.devnets).sort();
    }

    /** Historical devnets (excluding latest), sorted newest first */
    get historicalDevnets(): string[] {
        return this.availableDevnets.filter((d) => d !== this.latestDevnet).reverse();
    }

    /** Notebook configs sorted by order */
    get notebooks(): PQDevnetNotebookConfig[] {
        return [...this.config.notebooks].sort((a, b) => (a.order || 0) - (b.order || 0));
    }

    /** Get notebook data for a specific devnet and notebook ID */
    getNotebookData(devnetId: string, notebookId: string): PQDevnetNotebookData | undefined {
        return this.manifest.devnets?.[devnetId]?.[notebookId];
    }

    /** Check if a devnet has data */
    hasDevnet(devnetId: string): boolean {
        return !!this.manifest.devnets?.[devnetId];
    }

    /** Get notebook config by ID */
    getNotebook(id: string): PQDevnetNotebookConfig | undefined {
        return this.config.notebooks.find((n) => n.id === id);
    }

    /** Check if there's any PQ Devnet data available */
    get hasData(): boolean {
        return this.availableDevnets.length > 0;
    }
}

// Singleton instance for helper function compatibility
let pqDevnetSiteDataInstance: PQDevnetSiteData | null = null;

function getPQDevnetSiteData(): PQDevnetSiteData {
    if (!pqDevnetSiteDataInstance) {
        pqDevnetSiteDataInstance = PQDevnetSiteData.load();
    }
    return pqDevnetSiteDataInstance;
}

// Helper functions
export function getPQDevnetNotebooks(): PQDevnetNotebookConfig[] {
    return getPQDevnetSiteData().notebooks;
}

export function getLatestDevnet(): string {
    return getPQDevnetSiteData().latestDevnet;
}

export function getAvailableDevnets(): string[] {
    return getPQDevnetSiteData().availableDevnets;
}

export function hasPQDevnetData(): boolean {
    return getPQDevnetSiteData().hasData;
}
