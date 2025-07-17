from typing import Tuple, Dict
from pm4py.read import read_ocel2
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from pm4py import visualization as vis
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy, SlidingWindowStrategy, \
    LossyCountingStrategy
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pybeamline.models.ocdfg import OCDFG
from pybeamline.sources.ocel_log_source_from_file import ocel_log_source_from_file


logs = {"Logistics": {"filename": "../tests/logistics.jsonocel",
                      "parameters": [0.02],
                      "color": "#ff7f0e"}}


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
    img = vis.ocel.ocdfg.variants.classic.apply(ocdfg_offline_discovery, parameters={"format": "pdf", "filename": f"plots/{log}_ocdfg.pdf"})
    #print(img)
    # save the visualization

    # Convert the PM4Py OCDFG to a set of edges
    ocdfg_edges_pm4py = conform_ocdfg(ocdfg_offline_discovery)
    logs[log]["pm4py"] = ocdfg_edges_pm4py

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

        inclusion_strategy = RelativeFrequencyBasedStrategy(frequency_threshold=param)
        source.pipe(
            oc_operator(default_miner=default_miner, inclusion_strategy=inclusion_strategy),
            oc_merge_operator(),
        ).subscribe(lambda snapshot: handle_snapshot(snapshot, log, param))



# Visualize OCDFG at 6000
from pybeamline.utils.visualizer import Visualizer
visualizer = Visualizer()
print(logs["Logistics"][0.02]["snapshots"].keys())
visualizer.save(logs["Logistics"][0.02]["snapshots"][6029])