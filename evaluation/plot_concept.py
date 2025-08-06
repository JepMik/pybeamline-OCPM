import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
# Read Results
path = "jaccard_similarities/"
logs = ["Order Management", "P2P"]
colors = {"Logistics": "#ff7f0e",
            "Order Management": "#1f77b4",
            "P2P": "#2ca02c"}
threshold = 125
type = "SlidingWindow"


def read_jaccard_similarities(logs: list, type: str, threshold: str):
    similarities = {}
    file_path = f"{path}Concept_Drift_{type}_{threshold}.csv"
    for log_name in logs:
        similarities[log_name] = {}
    print(f"Reading Jaccard similarities from: {file_path}")
    with open(file_path, 'r') as file:
        lines = file.readlines()
    for line in lines:
        # Skip the header line
        if line.startswith("Event Count"):
            continue
        event_count, P2P_Jacc, OM_Jacc  = line.strip().split(',')
        similarities["P2P"][event_count] = float(P2P_Jacc)
        similarities["Order Management"][event_count] = float(OM_Jacc)
    return similarities

res = {}
similarities = read_jaccard_similarities(logs, type, str(threshold))

def plot_combined_jaccard(similarities: dict, colors: dict, title: str, save_path: str = None):
    data = []
    for log_name, values in similarities.items():
        color = colors.get(log_name, "#333333")
        for event_count, similarity in values.items():
            data.append({
                "Event Count": int(event_count),  # ensure integer x-axis
                "Jaccard Similarity": similarity,
                "Log": log_name,
                "Color": color
            })

    df = pd.DataFrame(data)
    plt.figure(figsize=(7, 3))  # shorter height

    for log_name in df["Log"].unique():
        subset = df[df["Log"] == log_name]
        color = colors.get(log_name, None)
        sns.lineplot(data=subset, x="Event Count", y="Jaccard Similarity", label=log_name, color=color)

    plt.title(title, fontsize=16)
    plt.xlabel("Events", fontsize=14)
    plt.ylabel("Jaccard Similarity", fontsize=14)
    plt.grid(True)
    plt.legend(title="Log")
    plt.tight_layout()

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(f"{save_path}.svg", format="svg")
    plt.savefig(f"{save_path}.pdf", format="pdf")
    print(f"Saved plot to {save_path}.svg and .pdf")
    plt.close()

plot_combined_jaccard(
    similarities=similarities,
    colors=colors,
    title=f"Concept Drift - (Window Size: {threshold})",
    save_path=f"plots/Concept_Drift_{type}_{threshold}"
)


