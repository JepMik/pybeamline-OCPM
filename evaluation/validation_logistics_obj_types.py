from typing import Tuple, Dict
import pandas as pd
from matplotlib import pyplot as plt
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pm4py import read_ocel2
import seaborn as sns
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file

print("Convert and calculate Jaccard similarities pr object type")

logss = {"Logistics": {"filename": "../tests/logistics.jsonocel",
                      "parameters": [0.01]}
        }

print(logss)

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

# Convert the PM4Py OCDFG to a set of edges
def edges_pr_object_type(ocdfg: set[Tuple[str,str,str]]) -> Dict[str, set[Tuple[str, str, str]]]:
    """
    Convert OCDFG to a dictionary of edges per object type.
    """
    result = {}
    for src, obj_type, tgt in ocdfg:
        if obj_type not in result:
            result[obj_type] = set()
        result[obj_type].add((src,obj_type, tgt))
    return result


for log in logss:
    log_file = logss[log]["filename"]
    # Read the OCDFG from the log file
    ocdfg_pm4py = read_ocel2(log_file)
    ocdfg_offline_discovery = ocdfg_discovery.apply(ocdfg_pm4py)
    # Convert the PM4Py OCDFG to a set of edges
    ocdfg_edges_pm4py = conform_ocdfg(ocdfg_offline_discovery)
    edges = edges_pr_object_type(ocdfg_edges_pm4py)
    print(edges)
    logss[log]["pm4py"] = edges


for log in logss:
    for param in logss[log]["parameters"]:
        if param not in logss[log]:
            logss[log][param] = {}
        logss[log][param]["snapshots"] = {}

        print("Processing log:", log, "with parameter:", param)
        event_count = 0


        def handle_snapshot(snapshot, log_name: str, param):
            global event_count
            if snapshot.get("type") == "event":
                event_count += 1
            if snapshot.get("ocdfg") is not None:
                logss[log_name][param]["snapshots"][event_count] = edges_pr_object_type(conform_emit_ocdfg(snapshot["ocdfg"]))


        source = ocel_log_source_from_file(logss[log]["filename"])
        default_miner = lambda: heuristics_miner_lossy_counting(model_update_frequency=1)

        inclusion_strategy = RelativeFrequencyBasedStrategy(frequency_threshold=param)
        source.pipe(
            oc_operator(default_miner=default_miner, inclusion_strategy=inclusion_strategy),
            oc_merge_operator(),
        ).subscribe(lambda snapshot: handle_snapshot(snapshot, log, param))

print("Convert and calculate Jaccard similarities for multiple logs...")

for log in logss:
    for param in logss[log]["parameters"]:
        for event_count, edges_obj_dict in logss[log][param]["snapshots"].items():
            if event_count > 9000:
                continue
            # Calculate Jaccard similarity using PM4Py object types as the baseline
            jaccard_similarities = {}
            for obj_type, ref_edges in logss[log]["pm4py"].items():
                discovered_edges = edges_obj_dict.get(obj_type, set())
                jaccard_similarities[obj_type] = jaccard_similarity(discovered_edges, ref_edges)

            if event_count not in logss[log]:
                logss[log][event_count] = {}

            logss[log][event_count]["jaccard_similarities"] = jaccard_similarities
            print(jaccard_similarities)


def plot_jaccard_per_object_type(log_data: dict, log_name: str):
    """
    Generates a line plot showing Jaccard Similarity per object type over event counts.

    Parameters:
    - log_data: Dictionary containing computed Jaccard similarities per event count.
    - log_name: Name of the log to use in the plot title.
    """
    # Define fixed colors for known object types
    object_type_colors = {
        "Transport Document": "#1f77b4",  # blue
        "Vehicle": "#ff7f0e",  # orange
        "Forklift": "#2ca02c",  # green
        "Truck": "#d62728",  # red
        "Container": "#9467bd",  # purple
        "Handling Unit": "#8c564b",  # brown
        "Customer Order": "#e377c2",  # pink/magenta
    }

    # Build data for plotting
    plot_data = []
    for event_count, data in log_data.items():
        if not isinstance(data, dict) or "jaccard_similarities" not in data:
            continue
        for obj_type, sim in data["jaccard_similarities"].items():
            plot_data.append({
                "Event Count": event_count,
                "Object Type": obj_type,
                "Jaccard Similarity": sim
            })

    df = pd.DataFrame(plot_data)

    # Plot
    plt.figure(figsize=(10, 5))
    sns.lineplot(
        data=df,
        x="Event Count",
        y="Jaccard Similarity",
        hue="Object Type",
        palette=object_type_colors
    )
    plt.title(f"{log_name}: Jaccard Similarity per Object Type - (Relative Frequency: 0.01)")
    plt.xlabel("Events")
    plt.ylabel("Jaccard Similarity")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

plot_jaccard_per_object_type(logss["Logistics"], "Logistics")