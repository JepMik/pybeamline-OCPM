import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
# Read Results
path = "jaccard_similarities/"
logs = ["Logistics", "Order Management", "P2P"]
colors = {"Logistics": "#ff7f0e",
            "Order Management": "#1f77b4",
            "P2P": "#2ca02c"}
threshold = 0.05
type = "max_approx"


def read_jaccard_similarities(log_name: str, type: str, threshold: str):
    similarities = {}
    file_path = f"{path}{log_name}_{type}_{threshold}.csv"
    print(f"Reading Jaccard similarities from: {file_path}")
    with open(file_path, 'r') as file:
        lines = file.readlines()
    for line in lines:
        # Skip the header line
        if line.startswith("Event Count,Jaccard Similarity"):
            continue
        event_count, similarity = line.strip().split(',')
        similarities[event_count] = float(similarity)
    return similarities

res = {}
for log in logs:
    similarities = read_jaccard_similarities(log, type, str(threshold))
    res[log] = similarities


def plot_jaccard_similarity_multiple_logs(logs: dict, title: str, max_event_count: int = 9000,
                                          save_path: str = "plots/jaccard_plot"):
    """
    Plots Jaccard similarity for multiple logs and saves as SVG and PDF.

    Args:
        logs (dict): A dict mapping log names to their similarity data (event_count → similarity).
        title (str): Title of the plot.
        max_event_count (int): Optional cutoff for max number of events.
        save_path (str): Path without file extension to save the plot (e.g., "plots/myplot").
    """
    plot_data = []

    for log_name, similarities in logs.items():
        for event_count_str, similarity in similarities.items():
            event_count = int(event_count_str)
            if event_count <= max_event_count:
                plot_data.append({
                    "Event Count": event_count,
                    "Jaccard Similarity": similarity,
                    "Log": log_name
                })

    df = pd.DataFrame(plot_data)

    plt.figure(figsize=(7, 3))  # Reduced height from e.g., 6 to 4

    for log_name in df["Log"].unique():
        log_df = df[df["Log"] == log_name]
        color = colors.get(log_name, None)
        sns.lineplot(data=log_df, x="Event Count", y="Jaccard Similarity", label=log_name, color=color)

    plt.title(title, fontsize=16)
    plt.xlabel("Events", fontsize=14)
    plt.ylabel("Jaccard Similarity", fontsize = 14)
    plt.legend(title="Log")
    plt.grid(True)
    plt.tight_layout()

    # Save as SVG and PDF
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(f"{save_path}.svg", format="svg")
    plt.savefig(f"{save_path}.pdf", format="pdf")
    plt.close()

plot_jaccard_similarity_multiple_logs(res, f"Jaccard Similarity - (Approximation Error: {threshold})", 9000, f"plots/Jacc_Lossy_App_{threshold}")

