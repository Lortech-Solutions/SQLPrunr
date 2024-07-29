import seaborn as sns
import matplotlib.pyplot as plt

from sqlprunr.data.query_data import Frequencies


def visualize_frequencies(
    frequencies: Frequencies,
    *,
    savefig: bool = False,
    figsize: tuple = (20, 120),
    show_tables: bool = True,
    show_columns: bool = True,
    show_queries: bool = False,
):
    fig, axes = plt.subplots(sum([show_tables, show_columns, show_queries]), 1, figsize=figsize)

    if show_tables:
        ax1 = axes[0]
        sns.barplot(x=list(frequencies.tables.values()), y=list(frequencies.tables.keys()), ax=ax1)
        ax1.set_title("Table Frequencies")
        ax1.set_xlabel("Frequency")
        ax1.set_ylabel(f"Tables ({len(list(frequencies.tables.keys()))} unique tables)")
        ax1.tick_params(axis="y", labelsize=12)

    if show_columns:
        ax2 = axes[1] if show_tables else axes[0]

        sns.barplot(x=list(frequencies.columns.values()), y=list(frequencies.columns.keys()), ax=ax2)
        ax2.set_title("Column Frequencies")
        ax2.set_xlabel("Frequency")
        ax2.set_ylabel(f"Columns ({len(list(frequencies.columns.keys()))} unique columns)")
        ax2.tick_params(axis="y", labelsize=8)

    if show_queries:
        ax3 = axes[2] if show_tables and show_columns else axes[0]
        
        sns.barplot(
            x=list(frequencies.queries.values()), y=list(frequencies.queries.keys()), ax=ax3
        )
        ax3.set_title("Query Frequencies")
        ax3.set_xlabel("Frequency")
        ax3.set_ylabel(f"Queries ({len(frequencies.queries)} unique queries)")
        ax3.tick_params(axis="y", labelsize=8)

    plt.tight_layout()
    if savefig:
        plt.savefig("frequencies.png")
