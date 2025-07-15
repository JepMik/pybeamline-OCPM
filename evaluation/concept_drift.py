from typing import Tuple

import pandas as pd
from matplotlib import pyplot as plt
from pm4py import read_ocel2
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from reactivex import concat
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy, SlidingWindowStrategy, \
    LossyCountingStrategy
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file
from pybeamline.models.ocdfg import OCDFG
import seaborn as sns

logs ={
        "P2P": {"filename": "../tests/ocel2-p2p.json",
                "color": "#2ca02c"},
        "Order Management": {"filename": "../tests/order-management.json",
                            "color": "#1f77b4"}
        }



def conform_ocdfg(ocdfg_pm4py) -> set[Tuple[str, str, str]]:
    """
    Convert PM4Py OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type in ocdfg_pm4py["edges"]["event_couples"].keys():
        for src, tgt in ocdfg_pm4py["edges"]["event_couples"][obj_type].keys():
            result.add((src, obj_type, tgt))
    return result

def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

def conform_emit_ocdfg(ocdfg: OCDFG) -> set[Tuple[str, str, str]]:
    """
    Convert OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type, transitions in ocdfg.edges.items():
        for (src, tgt), freq in transitions.items():
            result.add((src, obj_type, tgt))
    return result

for log in logs:
    log_file = logs[log]["filename"]
    # Read the OCDFG from the log file
    ocdfg_pm4py = read_ocel2(log_file)
    ocdfg_offline_discovery = ocdfg_discovery.apply(ocdfg_pm4py)
    # Convert the PM4Py OCDFG to a set of edges
    ocdfg_edges_pm4py = conform_ocdfg(ocdfg_offline_discovery)
    logs[log]["pm4py"] = ocdfg_edges_pm4py

print("Starting combine sources...")
# Combine sources
p2p_source =ocel_log_source_from_file("../tests/ocel2-p2p.json")
order_management_source = ocel_log_source_from_file("../tests/order-management.json")
combined_source = concat(p2p_source, order_management_source)




result = {}
event_count = 0
def handle_snapshot(snapshot, log_name: str):
    global event_count
    global result
    if snapshot.get("type") == "event":
        event_count += 1
    if snapshot.get("ocdfg") is not None:
        result[event_count] = conform_emit_ocdfg(snapshot["ocdfg"])


# Inclusion Strategy
inclusion_strategy = SlidingWindowStrategy(125)
miner = lambda: heuristics_miner_lossy_counting(model_update_frequency=1)

combined_source.pipe(
    oc_operator(inclusion_strategy=inclusion_strategy, default_miner=miner),
    oc_merge_operator()
).subscribe(lambda e: handle_snapshot(e, "Combined Logs"))

print("Compute similarities...")

similarities = {}
for event_count, ocdfg_edges in result.items():
    for log in logs:
        log_edges = logs[log]["pm4py"]
        similarity = jaccard_similarity(ocdfg_edges, log_edges)
        if log not in similarities:
            similarities[log] = {}
        similarities[log][event_count] = similarity
        print(f"Log: {log}, Event Count: {event_count}, Similarity: {similarity}")


# Simulated example of 'similarities' for illustration (replace with actual values)
# similarities = {
#     "P2P": {100: 0.4, 200: 0.5, 300: 0.6},
#     "Order Management": {100: 0.45, 200: 0.55, 300: 0.65}
# }

def plot_combined_jaccard(similarities: dict, logs: dict, title: str):
    """
    Plots Jaccard similarity over events for multiple logs using predefined colors.
    """
    data = []
    for log_name, values in similarities.items():
        color = logs[log_name]["color"]
        for event_count, similarity in values.items():
            data.append({
                "Event Count": event_count,
                "Jaccard Similarity": similarity,
                "Log": log_name,
                "Color": color
            })

    df = pd.DataFrame(data)
    plt.figure(figsize=(10, 6))

    for log_name in df["Log"].unique():
        subset = df[df["Log"] == log_name]
        color = logs[log_name]["color"]
        sns.lineplot(data=subset, x="Event Count", y="Jaccard Similarity", label=log_name, color=color)

    plt.title(title)
    plt.xlabel("Events")
    plt.ylabel("Jaccard Similarity")
    plt.grid(True)
    plt.legend(title="Log")
    plt.tight_layout()
    plt.show()


# Call function using actual similarities
plot_combined_jaccard(similarities, logs, "Jaccard Similarity Comparison Across Logs - (Window Size: 125)")