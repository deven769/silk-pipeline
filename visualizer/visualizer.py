import os
import matplotlib.pyplot as plt
from typing import Dict, List

class Visualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _format_labels(self, labels: List[str]) -> List[str]:
        """Format labels to ensure they fit well in the visualization."""
        return [label if len(label) <= 20 else f"{label[:17]}..." for label in labels]

    def _clean_labels(self, labels: List[str]) -> List[str]:
        """Replace empty or invalid labels with 'Unknown'."""
        return [label if label else "Unknown" for label in labels]

    # def _get_unique_filename(self, filename: str) -> str:
    #     """Generate a unique filename to avoid overwriting existing files."""
    #     base, extension = os.path.splitext(filename)
    #     counter = 1
    #     new_filename = filename
    #     while os.path.exists(os.path.join(self.output_dir, new_filename)):
    #         new_filename = f"{base}_{counter}{extension}"
    #         counter += 1
    #     return new_filename

    def _setup_bar_plot(self, labels: List[str], values: List[int], title: str, xlabel: str, ylabel: str) -> None:
        """Setup and configure a bar plot."""
        plt.figure(figsize=(10, 6))
        plt.bar(labels, values)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

    def _setup_pie_chart(self, labels: List[str], sizes: List[int], title: str) -> None:
        """Setup and configure a pie chart."""
        plt.figure(figsize=(8, 6))
        plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
        plt.title(title)
        plt.axis("equal")  # Equal aspect ratio ensures that pie is drawn as a circle.

    def _save_plot(self, filename: str) -> None:
        """Save the current plot to a file."""
        plt.savefig(os.path.join(self.output_dir, filename))
        plt.close()

    def visualize_os_distribution(self, os_distribution: Dict[str, int]) -> None:
        """Visualize the distribution of hosts by operating system."""
        labels = self._clean_labels(self._format_labels(list(os_distribution.keys())))
        values = list(os_distribution.values())
        self._setup_bar_plot(labels, values, "Distribution of Hosts by Operating System", "Operating System", "Number of Hosts")
        self._save_plot("os_distribution.png")

    def visualize_host_age_distribution(self, host_age_distribution: Dict[str, int]) -> None:
        """Visualize the distribution of host ages."""
        labels = self._clean_labels(self._format_labels(list(host_age_distribution.keys())))
        sizes = list(host_age_distribution.values())
        self._setup_pie_chart(labels, sizes, "Old Hosts vs Newly Discovered Hosts")
        self._save_plot("host_age_distribution.png")

    def visualize_cloud_provider_distribution(self, cloud_provider_distribution: Dict[str, int]) -> None:
        """Visualize the distribution of hosts by cloud provider."""
        labels = self._clean_labels(self._format_labels(list(cloud_provider_distribution.keys())))
        values = list(cloud_provider_distribution.values())
        self._setup_bar_plot(labels, values, "Distribution of Hosts by Cloud Provider", "Cloud Provider", "Number of Hosts")
        self._save_plot("cloud_provider_distribution.png")
