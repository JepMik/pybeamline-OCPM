from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting
from pybeamline.algorithms.oc.oc_merge_operator import oc_merge_operator
from pybeamline.algorithms.oc.oc_operator import oc_operator
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from pybeamline.utils.visualizer import Visualizer


test_stream = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
    {"activity": "Link Order To Shipment", "objects":  {"Order": ["o1"], "Shipment": ["s1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1", "i2"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i1", "i2"]}},
    {"activity": "Pack Item", "objects": {"Item": ["i1","i2"], "Order": ["o1"]}},
    {"activity": "Ship Item", "objects": {"Item": ["i1","i2"], "Shipment": ["s1"]}},
    {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
    {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
]

source = dict_test_ocel_source([(test_stream, 25)], shuffle=False)

emitted_ocdfgs = []
def handle_snapshot(snapshot):
    if snapshot.get("ocdfg") is not None:
        emitted_ocdfgs.append(snapshot["ocdfg"])

from reactivex import operators as ops
miner = lambda: heuristics_miner_lossy_counting(model_update_frequency=1, max_approx_error=0.01)
from pybeamline.algorithms.oc.strategies.base import RelativeFrequencyBasedStrategy
strategy = RelativeFrequencyBasedStrategy(frequency_threshold=0.001)
source.pipe(
    oc_operator(default_miner=miner, inclusion_strategy=strategy),
    oc_merge_operator(),
).subscribe(lambda e: handle_snapshot(e))

# Visualize the emitted OCDFGs
visualizer = Visualizer()
for i, m in enumerate(emitted_ocdfgs):
   visualizer.save(m)

visualizer.generate_ocdfg_gif(out_file="ocdfg_evolution.gif", duration=1000)
