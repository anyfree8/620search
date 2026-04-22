from dataclasses import dataclass, field as dc_field
from typing import Dict, Any, Optional

import os

DEFAULT_CONFIG_PATH = 'config/score.yaml'


@dataclass
class BM25Config:
    k1: float = 1.2
    b: float = 0.75
    field_weights: Dict[str, float] = dc_field(
        default_factory=lambda: {'title': 2.0, 'text': 1.0}
    )


@dataclass
class ProximityConfig:
    enabled: bool = True
    window: int = 8
    weight: float = 0.25
    field: str = 'text'


@dataclass
class ScoreConfig:
    bm25: BM25Config = dc_field(default_factory=BM25Config)
    proximity: ProximityConfig = dc_field(default_factory=ProximityConfig)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ScoreConfig':
        cfg = cls()
        bm25_d = (data or {}).get('bm25', {}) or {}
        if 'k1' in bm25_d:
            cfg.bm25.k1 = float(bm25_d['k1'])
        if 'b' in bm25_d:
            cfg.bm25.b = float(bm25_d['b'])
        fw = bm25_d.get('field_weights') or {}
        if fw:
            cfg.bm25.field_weights = {str(k): float(v) for k, v in fw.items()}

        prox_d = (data or {}).get('proximity', {}) or {}
        if 'enabled' in prox_d:
            cfg.proximity.enabled = bool(prox_d['enabled'])
        if 'window' in prox_d:
            cfg.proximity.window = int(prox_d['window'])
        if 'weight' in prox_d:
            cfg.proximity.weight = float(prox_d['weight'])
        if 'field' in prox_d:
            cfg.proximity.field = str(prox_d['field'])
        return cfg

    @classmethod
    def from_yaml(cls, file_path: str) -> 'ScoreConfig':
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required to load YAML config. Install with: pip install pyyaml")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
        return cls.from_dict(data)

    @classmethod
    def load(cls, file_path: Optional[str] = None) -> 'ScoreConfig':
        """Load from explicit path, DEFAULT_CONFIG_PATH if present, else defaults."""
        if file_path:
            return cls.from_yaml(file_path)
        if os.path.exists(DEFAULT_CONFIG_PATH):
            try:
                return cls.from_yaml(DEFAULT_CONFIG_PATH)
            except Exception:
                pass
        return cls()
