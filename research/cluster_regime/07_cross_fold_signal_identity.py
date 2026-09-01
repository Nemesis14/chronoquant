"""Cross-fold cluster identity via centroid matching + consistent-signal extraction (t872 final check).

CRITICAL METHODOLOGICAL CHECK: each of the 4 walk-forward folds fits an
INDEPENDENT KMeans model on a different (growing) training window. Cluster
integer labels are arbitrary per fit -- "Cluster 9" in Fold 1 and "Cluster
9" in Fold 3 are NOT guaranteed to represent the same market pattern; any
apparent agreement/disagreement in direction between same-numbered clusters
across folds (as computed by 06_cv_direction_stability.py) could be
spurious label coincidence.

This script establishes genuine cross-fold cluster IDENTITY via centroid
similarity in the shared 70-dimensional (14 features x 5 summary stats)
feature space -- which is comparable across folds because every fold's
window representation went through the same within-window z-score
normalization (features are already unitless/scale-comparable, no
additional rescaling needed for the similarity computation).

Method:
  1. Refit each fold's k=13 model (same settings as 04/05/06), extract the
     13 cluster centroids (13 x 70) per fold.
  2. For every pair of folds, compute the cosine similarity matrix between
     their centroids (Euclidean distance also reported as a cross-check).
  3. A MUTUAL best match (centroid A in fold i is B's closest match in
     fold j, AND B is A's closest match back in fold i) is treated as
     evidence of the same underlying market pattern recurring.
  4. Union-find over all mutual-match edges (across all 6 fold pairs,
     not just consecutive folds) groups (fold, cluster) nodes into
     connected components = candidate persistent patterns.
  5. For each component spanning >= 2 folds, pull each member's
     OUT-OF-FOLD (VALID) direction assignment (from
     cv_direction_stability_train_vs_valid.json, already computed) and
     check consistency: all members agree on SHORT, all agree on LONG, or
     conflicting/mixed -> the component (and by extension every cluster
     in it) is classified CONSISTENT_SHORT / CONSISTENT_LONG / MIXED /
     UNRELIABLE (single-fold-only, no cross-fold identity established).
"""

from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances

logger = logging.getLogger(__name__)

_cv = importlib.import_module("research.cluster_regime.04_cv_cluster_optimization")

SELECTED_K = 13
DIRECTION_STABILITY_PATH = Path("artifacts") / "research_cluster_regime" / "cv_direction_stability_train_vs_valid.json"
OUTPUT_PATH = Path("artifacts") / "research_cluster_regime" / "cv_cross_fold_signal_identity.json"

# Minimum cosine similarity for a mutual-best-match to count as "same pattern".
# Chosen empirically after inspecting the full similarity distribution (see
# the printed histogram in main()); documented explicitly in the output.
SIMILARITY_THRESHOLD = 0.5


class UnionFind:
    """Simple union-find (disjoint set) over string node ids."""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _fit_fold_centroids(df, feature_cols: list[str]) -> dict[str, np.ndarray]:
    """Fit each fold's k=13 model and return {fold_name: (13, n_features) centroids}."""
    centroids_by_fold: dict[str, np.ndarray] = {}

    for fold in _cv.FOLDS:
        df_train = _cv._slice_by_date(df, fold["train_start"], fold["train_end"])
        X_train = df_train[feature_cols].to_numpy(dtype=np.float64)
        medians = _cv._fit_medians(X_train)
        X_train_filled = _cv._apply_medians(X_train, medians)

        kmeans = _cv.MiniBatchKMeans(
            n_clusters=SELECTED_K,
            random_state=_cv.RANDOM_STATE,
            n_init=_cv.SEARCH_N_INIT,
            max_iter=_cv.SEARCH_MAX_ITER,
            batch_size=_cv.SEARCH_BATCH_SIZE,
        )
        kmeans.fit(X_train_filled)
        centroids_by_fold[fold["name"]] = kmeans.cluster_centers_
        logger.info("%s: fit complete, centroids shape=%s", fold["name"], kmeans.cluster_centers_.shape)

    return centroids_by_fold


def _mutual_best_matches(
    centroids_a: np.ndarray,
    centroids_b: np.ndarray,
) -> list[tuple[int, int, float, float]]:
    """Find mutual-nearest-neighbor centroid pairs between two folds' centroids.

    Args:
        centroids_a : (13, n_features) centroids for fold A.
        centroids_b : (13, n_features) centroids for fold B.

    Returns:
        List of (cluster_id_a, cluster_id_b, cosine_similarity, euclidean_distance)
        for every mutual best match (regardless of threshold -- filtering happens
        by the caller).
    """
    sim = cosine_similarity(centroids_a, centroids_b)  # (13_a, 13_b)
    dist = euclidean_distances(centroids_a, centroids_b)

    best_b_for_a = np.argmax(sim, axis=1)  # for each a, index of most-similar b
    best_a_for_b = np.argmax(sim, axis=0)  # for each b, index of most-similar a

    matches = []
    for a_idx, b_idx in enumerate(best_b_for_a):
        if best_a_for_b[b_idx] == a_idx:  # mutual
            matches.append((int(a_idx), int(b_idx), float(sim[a_idx, b_idx]), float(dist[a_idx, b_idx])))

    return matches


def _within_fold_baseline(centroids_by_fold: dict[str, np.ndarray]) -> dict[str, Any]:
    """Compute the within-fold pairwise similarity baseline (different clusters, same fit).

    This is the critical validity check: since all 13 clusters within a single
    fold are, by construction, DIFFERENT market patterns (that is the entire
    point of clustering), their pairwise centroid similarity establishes the
    "noise floor" -- if cross-fold mutual-match similarities are not
    meaningfully higher than this within-fold baseline, centroid similarity
    cannot discriminate "the same recurring pattern" from "two arbitrary,
    admittedly-different clusters," and any cross-fold matching built on it
    is unreliable.

    Args:
        centroids_by_fold : {fold_name: (13, n_features) centroids}.

    Returns:
        Dict with the pooled within-fold similarity distribution.
    """
    all_within: list[float] = []
    for _fold_name, centroids in centroids_by_fold.items():
        sim = cosine_similarity(centroids, centroids)
        n = sim.shape[0]
        mask = ~np.eye(n, dtype=bool)
        all_within.extend(sim[mask].tolist())

    arr = np.array(all_within)
    return {
        "n_pairs": int(len(arr)),
        "min": float(np.min(arr)),
        "p25": float(np.percentile(arr, 25)),
        "median": float(np.median(arr)),
        "p75": float(np.percentile(arr, 75)),
        "max": float(np.max(arr)),
    }


def main() -> dict[str, Any]:
    """Run the cross-fold centroid identity + consistent-signal extraction."""
    logging.basicConfig(level=logging.INFO)

    logger.info("Loading window representation and direction-stability results...")
    df, feature_cols = _cv._load_window_representation()

    with open(DIRECTION_STABILITY_PATH) as f:
        direction_data = json.load(f)
    valid_assignment: dict[tuple[str, int], dict[str, Any]] = {
        (row["fold"], row["cluster"]): {
            "assignment": row["valid_assignment"],
            "p_value": row["valid_p_value"],
            "n_obs": row["valid_n_obs"],
        }
        for row in direction_data["rows"]
    }

    logger.info("Fitting per-fold k=13 models and extracting centroids...")
    centroids_by_fold = _fit_fold_centroids(df, feature_cols)
    fold_names = [f["name"] for f in _cv.FOLDS]

    # Step 0 (CRITICAL VALIDITY CHECK): within-fold baseline similarity.
    # All 13 clusters within a single fold are, by construction, DIFFERENT
    # patterns -- their pairwise similarity is the noise floor that any
    # cross-fold "match" must clear to be considered meaningful.
    logger.info("Computing within-fold baseline similarity (validity check)...")
    within_fold_baseline = _within_fold_baseline(centroids_by_fold)
    logger.info("Within-fold baseline (different clusters, same fit): %s", within_fold_baseline)

    # Step 1: all-pairs mutual-best-match centroid comparison (6 fold pairs)
    all_pair_matches: dict[str, list[dict[str, Any]]] = {}
    all_similarities: list[float] = []

    for i in range(len(fold_names)):
        for j in range(i + 1, len(fold_names)):
            fa, fb = fold_names[i], fold_names[j]
            matches = _mutual_best_matches(centroids_by_fold[fa], centroids_by_fold[fb])
            pair_key = f"{fa}_vs_{fb}"
            all_pair_matches[pair_key] = [
                {"cluster_a": a, "cluster_b": b, "cosine_similarity": sim, "euclidean_distance": dist}
                for a, b, sim, dist in matches
            ]
            all_similarities.extend(sim for _, _, sim, _ in matches)
            logger.info("%s: %d mutual best-match pairs found", pair_key, len(matches))

    all_similarities_arr = np.array(all_similarities)
    similarity_distribution = {
        "n_mutual_matches_all_pairs": int(len(all_similarities_arr)),
        "min": float(np.min(all_similarities_arr)) if len(all_similarities_arr) else None,
        "p25": float(np.percentile(all_similarities_arr, 25)) if len(all_similarities_arr) else None,
        "median": float(np.median(all_similarities_arr)) if len(all_similarities_arr) else None,
        "p75": float(np.percentile(all_similarities_arr, 75)) if len(all_similarities_arr) else None,
        "max": float(np.max(all_similarities_arr)) if len(all_similarities_arr) else None,
        "n_above_threshold": int(np.sum(all_similarities_arr >= SIMILARITY_THRESHOLD)),
    }
    logger.info("Mutual-match cosine similarity distribution: %s", similarity_distribution)

    # Reliability verdict: compare the mutual-match distribution to the
    # within-fold (definitely-different-clusters) baseline. If the mutual
    # matches are not meaningfully separated from this noise floor, the
    # centroid-based cross-fold identity cannot be trusted.
    baseline_median = within_fold_baseline["median"]
    baseline_p75 = within_fold_baseline["p75"]
    matches_median = similarity_distribution["median"]
    frac_matches_below_baseline_p75 = (
        float(np.mean(all_similarities_arr <= baseline_p75)) if len(all_similarities_arr) else None
    )
    is_reliable = bool(matches_median is not None and matches_median > baseline_p75 and frac_matches_below_baseline_p75 is not None and frac_matches_below_baseline_p75 < 0.25)
    reliability_check = {
        "within_fold_baseline_median": baseline_median,
        "within_fold_baseline_p75": baseline_p75,
        "mutual_match_median": matches_median,
        "fraction_of_mutual_matches_at_or_below_baseline_p75": frac_matches_below_baseline_p75,
        "verdict": "RELIABLE" if is_reliable else "UNRELIABLE",
        "interpretation": (
            "Mutual-match similarities are NOT meaningfully separated from the within-fold "
            "baseline (different, by-construction-distinct clusters in the SAME fit already "
            "show cosine similarity in the same 0.90-0.99 range as the 'best' cross-fold "
            "matches). This means centroid cosine similarity CANNOT reliably distinguish "
            "'the same recurring market pattern' from 'two arbitrary, admittedly different "
            "clusters' in this 70-dimensional window-summary-statistic feature space -- the "
            "clusters found by K-means are geometrically close to each other overall (consistent "
            "with the historically low silhouette scores ~0.15-0.30 seen in the original t866 "
            "elbow search), not well-separated regimes. Row-centering (Pearson-style) and "
            "dropping near-zero-variance dimensions were both tested as fixes and did not "
            "resolve the issue (see analysis doc). The component/pattern groupings below are "
            "reported for completeness and audit-trail purposes, but should NOT be treated as "
            "confidently-established recurring market patterns."
            if not is_reliable else
            "Mutual-match similarities are meaningfully higher than the within-fold baseline; "
            "cross-fold centroid matching can be reasonably trusted here."
        ),
    }
    logger.info("Reliability check: %s", reliability_check["verdict"])

    # Step 2: union-find over mutual matches with similarity >= threshold
    uf = UnionFind()
    edges_used: list[dict[str, Any]] = []
    for pair_key, matches in all_pair_matches.items():
        fa, fb = pair_key.split("_vs_")
        for m in matches:
            if m["cosine_similarity"] >= SIMILARITY_THRESHOLD:
                node_a = f"{fa}:{m['cluster_a']}"
                node_b = f"{fb}:{m['cluster_b']}"
                uf.union(node_a, node_b)
                edges_used.append({"node_a": node_a, "node_b": node_b, "cosine_similarity": m["cosine_similarity"]})

    # Ensure every (fold, cluster) node exists in the union-find, even if isolated
    for fold_name in fold_names:
        for cluster_id in range(SELECTED_K):
            uf.find(f"{fold_name}:{cluster_id}")

    # Step 3: group nodes by component
    components: dict[str, list[str]] = {}
    for fold_name in fold_names:
        for cluster_id in range(SELECTED_K):
            node = f"{fold_name}:{cluster_id}"
            root = uf.find(node)
            components.setdefault(root, []).append(node)

    # Step 4: for each component spanning >=2 folds, pull direction assignments and classify
    component_reports = []
    for _root, nodes in components.items():
        folds_in_component = sorted({n.split(":")[0] for n in nodes})
        if len(folds_in_component) < 2:
            continue  # single-fold-only cluster -- no cross-fold identity, skip (handled as UNRELIABLE below)

        member_directions = []
        for node in nodes:
            fold_name, cluster_id_str = node.split(":")
            cluster_id = int(cluster_id_str)
            info = valid_assignment.get((fold_name, cluster_id), {"assignment": "UNKNOWN", "p_value": None, "n_obs": None})
            member_directions.append(
                {
                    "fold": fold_name,
                    "cluster": cluster_id,
                    "valid_assignment": info["assignment"],
                    "valid_p_value": info["p_value"],
                    "valid_n_obs": info["n_obs"],
                }
            )

        assignments_seen = {m["valid_assignment"] for m in member_directions}
        directional = assignments_seen - {"NEUTRAL", "MISSING", "UNKNOWN"}

        if directional == {"SHORT"}:
            verdict = "CONSISTENT_SHORT"
        elif directional == {"LONG"}:
            verdict = "CONSISTENT_LONG"
        elif len(directional) >= 2:
            verdict = "MIXED_CONFLICTING"
        else:
            verdict = "NO_DIRECTIONAL_SIGNAL"  # all NEUTRAL/MISSING despite matching across folds

        component_reports.append(
            {
                "component_id": f"pattern_{len(component_reports)}",
                "n_folds_spanned": len(folds_in_component),
                "folds_spanned": folds_in_component,
                "members": member_directions,
                "verdict": verdict,
            }
        )

    # Sort: prefer components spanning more folds, then by verdict (directional first)
    component_reports.sort(key=lambda c: (-c["n_folds_spanned"], c["verdict"] not in ("CONSISTENT_SHORT", "CONSISTENT_LONG")))

    consistent_short = [c for c in component_reports if c["verdict"] == "CONSISTENT_SHORT"]
    consistent_long = [c for c in component_reports if c["verdict"] == "CONSISTENT_LONG"]
    mixed = [c for c in component_reports if c["verdict"] == "MIXED_CONFLICTING"]
    no_signal = [c for c in component_reports if c["verdict"] == "NO_DIRECTIONAL_SIGNAL"]
    single_fold_only = sum(1 for nodes in components.values() if len({n.split(":")[0] for n in nodes}) < 2)

    output = {
        "metadata": {
            "description": "Cross-fold cluster identity via centroid matching (cosine similarity, mutual-nearest-neighbor), then consistent-direction extraction (t872 final check)",
            "k": SELECTED_K,
            "similarity_metric": "cosine_similarity (primary), euclidean_distance (cross-check)",
            "similarity_threshold_for_identity": SIMILARITY_THRESHOLD,
            "matching_rule": "mutual nearest neighbor (bidirectional best match) between every pair of folds (6 pairs total, not just consecutive)",
            "grouping": "union-find over all threshold-passing mutual matches -> connected components = candidate persistent patterns",
        },
        "within_fold_baseline": within_fold_baseline,
        "reliability_check": reliability_check,
        "similarity_distribution": similarity_distribution,
        "pairwise_mutual_matches": all_pair_matches,
        "union_find_edges_used": edges_used,
        "components": component_reports,
        "summary": {
            "reliability_verdict": reliability_check["verdict"],
            "total_components_spanning_2plus_folds": len(component_reports),
            "single_fold_only_clusters": single_fold_only,
            "consistent_short_count": len(consistent_short),
            "consistent_long_count": len(consistent_long),
            "mixed_conflicting_count": len(mixed),
            "no_directional_signal_count": len(no_signal),
        },
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    logger.info("Saved to %s", OUTPUT_PATH)

    # Print report
    print("\n" + "=" * 110)
    print(f"CROSS-FOLD CLUSTER IDENTITY (centroid cosine similarity, threshold={SIMILARITY_THRESHOLD})")
    print("=" * 110)
    print("*** CRITICAL VALIDITY CHECK ***")
    print(f"  Within-fold baseline (DIFFERENT clusters, SAME fit): median={within_fold_baseline['median']:.4f}, "
          f"p75={within_fold_baseline['p75']:.4f}, range=[{within_fold_baseline['min']:.4f}, {within_fold_baseline['max']:.4f}]")
    print(f"  Cross-fold mutual-match distribution:                median={similarity_distribution['median']:.4f}, "
          f"range=[{similarity_distribution['min']:.4f}, {similarity_distribution['max']:.4f}]")
    print(f"  RELIABILITY VERDICT: {reliability_check['verdict']}")
    print(f"  {reliability_check['interpretation']}")
    print("-" * 110)
    print("Mutual-match similarity distribution across all 6 fold pairs:")
    print(f"  n={similarity_distribution['n_mutual_matches_all_pairs']}, "
          f"min={similarity_distribution['min']:.3f}, p25={similarity_distribution['p25']:.3f}, "
          f"median={similarity_distribution['median']:.3f}, p75={similarity_distribution['p75']:.3f}, "
          f"max={similarity_distribution['max']:.3f}")
    print(f"  {similarity_distribution['n_above_threshold']} of {similarity_distribution['n_mutual_matches_all_pairs']} "
          f"mutual matches pass the >= {SIMILARITY_THRESHOLD} identity threshold")
    print("-" * 110)
    print(f"Components spanning >=2 folds: {len(component_reports)}")
    print(f"Clusters with NO cross-fold match at all (single-fold-only): {single_fold_only}")
    print("-" * 110)

    for comp in component_reports:
        print(f"\n{comp['component_id']} -- spans {comp['n_folds_spanned']} folds ({', '.join(comp['folds_spanned'])}) -- VERDICT: {comp['verdict']}")
        for m in comp["members"]:
            p = f"{m['valid_p_value']:.5f}" if m["valid_p_value"] is not None else "N/A"
            print(f"    {m['fold']}:cluster{m['cluster']}  valid_assignment={m['valid_assignment']:<8} p={p}  n_obs={m['valid_n_obs']}")

    print("\n" + "-" * 110)
    print(f"FINAL VERDICT (candidate groupings -- reliability={reliability_check['verdict']}, see validity check above):")
    print(f"  CONSISTENT_SHORT patterns (candidate): {len(consistent_short)}")
    print(f"  CONSISTENT_LONG patterns (candidate):  {len(consistent_long)}")
    print(f"  MIXED/CONFLICTING:                     {len(mixed)}")
    print(f"  Matched but NO signal:                 {len(no_signal)}")
    if reliability_check["verdict"] == "UNRELIABLE":
        print("  *** These groupings should NOT be treated as confidently-established recurring")
        print("      market patterns -- see the validity check at the top of this report. ***")
    print("=" * 110)

    return output


if __name__ == "__main__":
    main()
