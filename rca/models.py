"""Optional learned RCA models with dependency-safe deterministic fallbacks."""

from __future__ import annotations

import json
import math
import pickle
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .dataset import feature_matrix, feature_schema


class OptionalDependencyError(RuntimeError):
    pass


class PortableLinearRanker:
    """Tiny supervised fallback used when scikit-learn is unavailable.

    It learns standardized positive-vs-negative mean differences.  It is not
    intended to beat boosted trees; it guarantees that offline training, replay,
    and demos remain functional in a minimal Python environment.
    """

    def __init__(self) -> None:
        self.schema: tuple[str, ...] = ()
        self.means: list[float] = []
        self.scales: list[float] = []
        self.weights: list[float] = []
        self.bias = 0.0

    def fit(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        feature_key: str = "features",
        label_key: str = "label",
    ) -> "PortableLinearRanker":
        if not records:
            raise ValueError("cannot train on an empty dataset")
        self.schema = feature_schema(records, feature_key=feature_key)
        matrix = feature_matrix(records, self.schema, feature_key=feature_key)
        labels = [int(bool(record.get(label_key))) for record in records]
        if len(set(labels)) < 2:
            raise ValueError("training requires positive and negative candidates")
        columns = list(zip(*matrix)) if self.schema else []
        self.means = [statistics.fmean(column) for column in columns]
        self.scales = [statistics.pstdev(column) or 1.0 for column in columns]
        positives = [row for row, label in zip(matrix, labels) if label]
        negatives = [row for row, label in zip(matrix, labels) if not label]
        self.weights = []
        for index in range(len(self.schema)):
            positive_mean = statistics.fmean(row[index] for row in positives)
            negative_mean = statistics.fmean(row[index] for row in negatives)
            self.weights.append((positive_mean - negative_mean) / self.scales[index])
        prior = (sum(labels) + 0.5) / (len(labels) + 1.0)
        self.bias = math.log(prior / (1.0 - prior))
        return self

    def predict_scores(
        self, records: Sequence[Mapping[str, Any]], *, feature_key: str = "features"
    ) -> list[float]:
        if not self.schema:
            raise RuntimeError("model is not fitted")
        matrix = feature_matrix(records, self.schema, feature_key=feature_key)
        scores: list[float] = []
        for row in matrix:
            logit = self.bias + sum(
                weight * ((value - mean) / scale)
                for value, mean, scale, weight in zip(row, self.means, self.scales, self.weights)
            )
            logit = max(-30.0, min(30.0, logit))
            scores.append(1.0 / (1.0 + math.exp(-logit)))
        return scores

    def save(self, path: str | Path) -> None:
        payload = {
            "kind": "portable-linear-v1",
            "schema": self.schema,
            "means": self.means,
            "scales": self.scales,
            "weights": self.weights,
            "bias": self.bias,
        }
        Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "PortableLinearRanker":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("kind") != "portable-linear-v1":
            raise ValueError("unsupported portable model artifact")
        model = cls()
        model.schema = tuple(payload["schema"])
        model.means = [float(value) for value in payload["means"]]
        model.scales = [float(value) for value in payload["scales"]]
        model.weights = [float(value) for value in payload["weights"]]
        model.bias = float(payload["bias"])
        return model


class ChampionRanker:
    """Candidate classifier backed by scikit-learn when installed."""

    def __init__(self, *, prefer_sklearn: bool = True, random_state: int = 42) -> None:
        self.prefer_sklearn = prefer_sklearn
        self.random_state = random_state
        self.schema: tuple[str, ...] = ()
        self.backend = "unfitted"
        self.model: Any = None

    def fit(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        feature_key: str = "features",
        label_key: str = "label",
    ) -> "ChampionRanker":
        if not records:
            raise ValueError("cannot train on an empty dataset")
        self.schema = feature_schema(records, feature_key=feature_key)
        matrix = feature_matrix(records, self.schema, feature_key=feature_key)
        labels = [int(bool(record.get(label_key))) for record in records]
        if len(set(labels)) < 2:
            raise ValueError("training requires positive and negative candidates")
        if self.prefer_sklearn:
            try:
                from sklearn.ensemble import HistGradientBoostingClassifier

                self.model = HistGradientBoostingClassifier(
                    learning_rate=0.06,
                    max_iter=200,
                    max_leaf_nodes=15,
                    l2_regularization=1.0,
                    random_state=self.random_state,
                ).fit(matrix, labels)
                self.backend = "sklearn-hist-gradient-boosting"
                return self
            except ImportError:
                pass
        self.model = PortableLinearRanker().fit(records, feature_key=feature_key, label_key=label_key)
        self.backend = "portable-linear"
        return self

    def predict_scores(
        self, records: Sequence[Mapping[str, Any]], *, feature_key: str = "features"
    ) -> list[float]:
        if self.backend == "unfitted":
            raise RuntimeError("model is not fitted")
        if self.backend == "portable-linear":
            return self.model.predict_scores(records, feature_key=feature_key)
        matrix = feature_matrix(records, self.schema, feature_key=feature_key)
        return [float(row[1]) for row in self.model.predict_proba(matrix)]

    def rank_by_incident(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        incident_key: str = "incident_id",
        candidate_key: str = "candidate_id",
        feature_key: str = "features",
    ) -> dict[str, list[tuple[str, float]]]:
        scores = self.predict_scores(records, feature_key=feature_key)
        grouped: dict[str, list[tuple[str, float]]] = {}
        for record, score in zip(records, scores):
            grouped.setdefault(str(record[incident_key]), []).append((str(record[candidate_key]), score))
        for incident in grouped:
            grouped[incident].sort(key=lambda item: (-item[1], item[0]))
        return grouped

    def save(self, path: str | Path) -> None:
        """Persist an artifact. Only load pickle artifacts produced by this project."""

        target = Path(path)
        if self.backend == "portable-linear":
            self.model.save(target)
            return
        payload = {
            "kind": "champion-pickle-v1",
            "backend": self.backend,
            "schema": self.schema,
            "model": self.model,
        }
        target.write_bytes(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))

    @classmethod
    def load(cls, path: str | Path) -> "ChampionRanker":
        target = Path(path)
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = pickle.loads(target.read_bytes())
        ranker = cls()
        if payload.get("kind") == "portable-linear-v1":
            ranker.model = PortableLinearRanker.load(target)
            ranker.schema = ranker.model.schema
            ranker.backend = "portable-linear"
        elif payload.get("kind") == "champion-pickle-v1":
            ranker.model = payload["model"]
            ranker.schema = tuple(payload["schema"])
            ranker.backend = str(payload["backend"])
        else:
            raise ValueError("unsupported model artifact")
        return ranker


try:  # Optional heavy dependency.  Importing rca.models must always remain safe.
    import torch
    from torch import Tensor, nn

    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover - environment dependent
    torch = None  # type: ignore[assignment]
    Tensor = Any  # type: ignore[misc,assignment]
    nn = None  # type: ignore[assignment]
    TORCH_AVAILABLE = False


if TORCH_AVAILABLE:

    class EdgeAwareGraphAttention(nn.Module):
        """Small multi-head directed GAT layer requiring only core PyTorch."""

        def __init__(self, hidden_dim: int, edge_dim: int, heads: int = 4, dropout: float = 0.1) -> None:
            super().__init__()
            if hidden_dim % heads:
                raise ValueError("hidden_dim must be divisible by heads")
            self.hidden_dim = hidden_dim
            self.heads = heads
            self.head_dim = hidden_dim // heads
            self.query = nn.Linear(hidden_dim, hidden_dim, bias=False)
            self.key = nn.Linear(hidden_dim, hidden_dim, bias=False)
            self.value = nn.Linear(hidden_dim, hidden_dim, bias=False)
            self.edge = nn.Linear(edge_dim, hidden_dim, bias=False)
            self.output = nn.Linear(hidden_dim, hidden_dim)
            self.dropout = nn.Dropout(dropout)
            self.norm = nn.LayerNorm(hidden_dim)

        def forward(
            self, nodes: Tensor, edge_index: Tensor, edge_features: Tensor
        ) -> tuple[Tensor, Tensor]:
            # nodes [B,N,D], edge_index [2,E], edge_features [B,E,Fe] or [E,Fe]
            batch, node_count, _ = nodes.shape
            source, target = edge_index[0].long(), edge_index[1].long()
            if edge_features.dim() == 2:
                edge_features = edge_features.unsqueeze(0).expand(batch, -1, -1)
            q = self.query(nodes).view(batch, node_count, self.heads, self.head_dim)
            k = self.key(nodes).view(batch, node_count, self.heads, self.head_dim)
            v = self.value(nodes).view(batch, node_count, self.heads, self.head_dim)
            e = self.edge(edge_features).view(batch, -1, self.heads, self.head_dim)
            logits = (q[:, target] * (k[:, source] + e)).sum(-1) / math.sqrt(self.head_dim)
            logits = torch.nn.functional.leaky_relu(logits, negative_slope=0.2)
            output = torch.zeros_like(q)
            attentions = torch.zeros_like(logits)
            for node in range(node_count):
                mask = target == node
                if not bool(mask.any()):
                    continue
                alpha = torch.softmax(logits[:, mask, :], dim=1)
                attentions[:, mask, :] = alpha
                messages = v[:, source[mask]] + e[:, mask]
                output[:, node] = (self.dropout(alpha).unsqueeze(-1) * messages).sum(dim=1)
            merged = output.reshape(batch, node_count, self.hidden_dim)
            return self.norm(nodes + self.output(merged)), attentions


    class GraphTemporalAttentionRanker(nn.Module):
        """Temporal Transformer + directed edge-aware attention RCA challenger.

        This is a compact numeric model trained from scratch.  It is not a language
        model and consumes no prompts or natural-language generation targets.
        """

        def __init__(
            self,
            node_feature_dim: int,
            edge_feature_dim: int,
            *,
            hidden_dim: int = 64,
            temporal_heads: int = 4,
            graph_heads: int = 4,
            temporal_layers: int = 2,
            graph_layers: int = 2,
            dropout: float = 0.1,
            max_steps: int = 256,
            fault_classes: int = 8,
        ) -> None:
            super().__init__()
            if hidden_dim % temporal_heads:
                raise ValueError("hidden_dim must be divisible by temporal_heads")
            self.input_projection = nn.Linear(node_feature_dim, hidden_dim)
            self.position = nn.Parameter(torch.zeros(1, max_steps, hidden_dim))
            layer = nn.TransformerEncoderLayer(
                d_model=hidden_dim,
                nhead=temporal_heads,
                dim_feedforward=hidden_dim * 4,
                dropout=dropout,
                activation="gelu",
                batch_first=True,
                norm_first=True,
            )
            self.temporal = nn.TransformerEncoder(layer, num_layers=temporal_layers)
            self.graph_layers = nn.ModuleList(
                [EdgeAwareGraphAttention(hidden_dim, edge_feature_dim, graph_heads, dropout) for _ in range(graph_layers)]
            )
            self.node_root_head = nn.Linear(hidden_dim, 1)
            self.edge_root_head = nn.Sequential(
                nn.Linear(hidden_dim * 2 + edge_feature_dim, hidden_dim),
                nn.GELU(),
                nn.Linear(hidden_dim, 1),
            )
            self.fault_head = nn.Linear(hidden_dim, fault_classes)
            self.abstention_head = nn.Linear(hidden_dim, 1)

        def forward(
            self,
            node_series: Tensor,
            edge_index: Tensor,
            edge_features: Tensor,
            *,
            padding_mask: Tensor | None = None,
            return_attention: bool = False,
        ) -> Mapping[str, Tensor]:
            # node_series [B,T,N,F]
            batch, steps, nodes, _ = node_series.shape
            if steps > self.position.shape[1]:
                raise ValueError("sequence is longer than configured max_steps")
            temporal = self.input_projection(node_series).permute(0, 2, 1, 3).reshape(batch * nodes, steps, -1)
            temporal = temporal + self.position[:, :steps]
            expanded_mask = None
            if padding_mask is not None:
                expanded_mask = padding_mask[:, None, :].expand(batch, nodes, steps).reshape(batch * nodes, steps)
            encoded = self.temporal(temporal, src_key_padding_mask=expanded_mask)
            state = encoded[:, -1].reshape(batch, nodes, -1)
            layer_attention: list[Tensor] = []
            for layer in self.graph_layers:
                state, attention = layer(state, edge_index, edge_features)
                layer_attention.append(attention)
            source, target = edge_index[0].long(), edge_index[1].long()
            expanded_edges = edge_features
            if expanded_edges.dim() == 2:
                expanded_edges = expanded_edges.unsqueeze(0).expand(batch, -1, -1)
            edge_state = torch.cat((state[:, source], state[:, target], expanded_edges), dim=-1)
            pooled = state.mean(dim=1)
            output: dict[str, Tensor] = {
                "node_logits": self.node_root_head(state).squeeze(-1),
                "edge_logits": self.edge_root_head(edge_state).squeeze(-1),
                "fault_logits": self.fault_head(state),
                "abstention_logit": self.abstention_head(pooled).squeeze(-1),
            }
            if return_attention:
                output["attention"] = torch.stack(layer_attention, dim=1)
            return output

else:

    class EdgeAwareGraphAttention:  # pragma: no cover - only used without torch
        def __init__(self, *_: Any, **__: Any) -> None:
            raise OptionalDependencyError("PyTorch is required; install requirements-ml.txt")


    class GraphTemporalAttentionRanker:  # pragma: no cover - only used without torch
        def __init__(self, *_: Any, **__: Any) -> None:
            raise OptionalDependencyError("PyTorch is required; install requirements-ml.txt")
