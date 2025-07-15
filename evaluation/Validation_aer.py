from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from river import drift
from reactivex import operators as ops
from reactivex import concat
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pybeamline.utils.cardinality import Cardinality

log_2 = [
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1", "i2"]}},
]

log_1 = [
    {"activity": "Add Item", "objects": {"Order": ["o2"], "Item": ["i3"]}},
]
result = {}
result["Add Item"] = {}
event_count = 0
def handle_snapshot(snapshot):
    global event_count
    global result
    if snapshot.get("type") == "event":
        event_count += 1
        if snapshot["event"].get_event_name() == "Add Item":
                for object_type, object_identifiers in snapshot["event"].get_omap().items():
                    if object_type == "Item":
                        result["Add Item"][event_count] = len(object_identifiers)
    if snapshot.get("type") == "aer":
        result[event_count] = snapshot["model"]


source1 = dict_test_ocel_source([(log_1, 25),(log_2, 10)], shuffle=True)
source2 = dict_test_ocel_source([(log_1, 10),(log_2, 50)], shuffle=True)
source = concat(source1, source2)


miner = lambda: heuristics_miner_lossy_counting(model_update_frequency=1)
source.pipe(
    oc_operator(default_miner=miner, aer_model_update_frequency=1, aer_model_max_approx_error=0.01),
).subscribe(lambda snapshot: handle_snapshot(snapshot))

for key, value in result.items():
    if key == "Add Item":
        continue
    for relation, card in value.get_relations("Add Item").items():
        if card == Cardinality.ONE_TO_ONE:
            result.update({key: 0})
        else:
            result.update({key: 1})



# Simulate result from the script logic
#result = {
#    "Add Item": {1: 2, 2: 1, 3: 2, 4: 1, 5: 2},
#    6: 1, 7: 0, 8: 1, 9: 0, 10: 1  # where 1 = Many-to-One, 0 = One-to-One
#}


item_counts = result["Add Item"]
cardinality_data = {k: v for k, v in result.items() if isinstance(k, int)}

# Prepare DataFrame
df_items = pd.DataFrame(item_counts.items(), columns=["Event", "Item Count"])
df_card = pd.DataFrame(cardinality_data.items(), columns=["Event", "Cardinality"])
df_card["Cardinality Label"] = df_card["Cardinality"].map({0: "One-to-One", 1: "Many-to-One"})

# Merge for plotting
df = pd.merge(df_items, df_card, on="Event", how="outer").sort_values("Event")

# Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

sns.scatterplot(data=df, x="Event", y="Item Count", ax=ax1, label="Item Count", color="tab:blue")
ax1.set_ylabel("Item Count", color="tab:blue")
ax1.set_ylim(0, 3)
ax1.set_xlabel("Event Count")
ax1.axvline(x=35, color='red', linestyle=':', linewidth=1.5, label='Concept Drift Point')
ax1.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray')
# Twin axis for cardinality
ax2 = ax1.twinx()
sns.lineplot(data=df, x="Event", y="Cardinality", ax=ax2, label="Cardinality", color="tab:orange")
ax2.legend_.remove()
ax2.set_yticks([0, 1])
ax2.set_yticklabels(["One-to-One", "Many-to-One"])
ax2.set_ylabel("Cardinality", color="tab:orange")


# Combine legends
lines_labels = ax1.get_legend_handles_labels()
lines_labels2 = ax2.get_legend_handles_labels()
lines = lines_labels[0] + lines_labels2[0]
labels = lines_labels[1] + lines_labels2[1]
ax1.legend(lines,labels, loc="lower right", title="Legend")

plt.title("Activity-Entity Relationship Miner with Lossy Counting - (Approximation Error: 0.01)")

plt.tight_layout()
plt.show()