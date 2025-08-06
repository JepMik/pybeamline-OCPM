from typing import Tuple, Dict
from pm4py.read import read_ocel2
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy, SlidingWindowStrategy, \
    LossyCountingStrategy
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file

logs = {"Logistics": {"filename": "../tests/logistics.jsonocel",
                      "parameters": [0.05],
                      "color": "#ff7f0e"},
        "P2P": {"filename": "../tests/ocel2-p2p.json",
                "parameters": [0.05],
                "color": "#2ca02c"},
        "Order Management": {"filename": "../tests/order-management.json",
                            "parameters": [0.05],
                            "color": "#1f77b4"},
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

def conform_emit_ocdfg(ocdfg: OCDFG) -> set[Tuple[str, str, str]]:
    """
    Convert OCDFG to a set of edges in the format (source, object_type, target).
    """
    result = set()
    for obj_type, transitions in ocdfg.edges.items():
        for (src, tgt), freq in transitions.items():
            result.add((src, obj_type, tgt))
    return result


def jaccard_similarity(model: set, ref_model: set) -> float:
    intersection = len(model.intersection(ref_model))
    union = len(model.union(ref_model))

    if union == 0:
        return 0.0  # Avoid division by zero

    return intersection / union

for log in logs:
    log_file = logs[log]["filename"]
    # Read the OCDFG from the log file
    ocdfg_pm4py = read_ocel2(log_file)
    ocdfg_offline_discovery = ocdfg_discovery.apply(ocdfg_pm4py)
    # Visualize the OCDFG

    # Convert the PM4Py OCDFG to a set of edges
    ocdfg_edges_pm4py = conform_ocdfg(ocdfg_offline_discovery)
    logs[log]["pm4py"] = ocdfg_edges_pm4py

print("Starting the streaming process for multiple logs...")

for log in logs:
    for param in logs[log]["parameters"]:
        if param not in logs[log]:
            logs[log][param] = {}
        logs[log][param]["snapshots"] = {}

        print("Processing log:", log, "with parameter:", param)
        event_count = 0
        def handle_snapshot(snapshot, log_name: str, param):
            # print(f"Received snapshot: {snapshot}")
            global event_count
            if snapshot.get("type") == "event":
                event_count += 1
            if snapshot.get("ocdfg") is not None:
                logs[log_name][param]["snapshots"][event_count] = snapshot["ocdfg"]

        source = ocel_log_source_from_file(logs[log]["filename"])
        default_miner = lambda : heuristics_miner_lossy_counting(model_update_frequency=1)

        inclusion_strategy = LossyCountingStrategy(max_approx_error=param)
        source.pipe(
            oc_operator(default_miner=default_miner, inclusion_strategy=inclusion_strategy),
            oc_merge_operator(),
        ).subscribe(lambda snapshot: handle_snapshot(snapshot, log, param))

print("Convert and calculate Jaccard similarities for multiple logs...")

for log in logs:
    for param in logs[log]["parameters"]:
        logs[log][param]["jaccard_similarities"] = {}
        for key, ocdg in logs[log][param]["snapshots"].items():
            # Convert the OCDFG to a set of edges
            ocdfg_edges = conform_emit_ocdfg(ocdg)
            logs[log][param]["snapshots"][key] = ocdfg_edges

        for key, ocdfg_edges in logs[log][param]["snapshots"].items():
            # Calculate Jaccard similarities for each snapshot against the PM4Py OCDFG
            jaccard_sim = jaccard_similarity(ocdfg_edges, logs[log]["pm4py"])
            logs[log][param]["jaccard_similarities"][key] = jaccard_sim


# Save the Jaccard similarities to a CSV file
import os
import csv
if not os.path.exists("jaccard_similarities"):
    os.makedirs("jaccard_similarities")

for log_name, log_data in logs.items():
    for param in log_data["parameters"]:
        filename = f"jaccard_similarities/{log_name}_max_approx_{param}.csv"
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Event Count', 'Jaccard Similarity']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for event_count, similarity in log_data[param]["jaccard_similarities"].items():
                writer.writerow({'Event Count': event_count, 'Jaccard Similarity': similarity})

print("About to plot Jaccard similarities for multiple logs...")

def plot_jaccard_similarity_multiple_logs(logs: dict, threshold: float, title: str, max_event_count: int = 9000):
    """
    Creates a seaborn line plot for multiple logs, one line per log,
    showing Jaccard similarity over event count for a specific threshold.
    Limits the x-axis to a maximum number of events.
    """
    plot_data = []

    for log_name, log_data in logs.items():
        if threshold not in log_data:
            continue
        jaccard_data = log_data[threshold]["jaccard_similarities"]
        for event_count, similarity in jaccard_data.items():
            if event_count <= max_event_count:
                plot_data.append({
                    "Event Count": event_count,
                    "Jaccard Similarity": similarity,
                    "Log": log_name,
                    "Color": log_data["color"]
                })

    df = pd.DataFrame(plot_data)
    plt.figure(figsize=(10, 6))

    for log_name in df["Log"].unique():
        log_df = df[df["Log"] == log_name]
        color = logs[log_name]["color"]
        sns.lineplot(data=log_df, x="Event Count", y="Jaccard Similarity",
                     label=log_name, color=color)

    plt.title(title)
    plt.xlabel("Events")
    plt.ylabel("Jaccard Similarity")
    plt.legend(title="Log")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

for param in logs["Logistics"]["parameters"]:
    plot_jaccard_similarity_multiple_logs(logs, threshold=param, title= f"Jaccard Similarity - (Approximation Error: {param})")



