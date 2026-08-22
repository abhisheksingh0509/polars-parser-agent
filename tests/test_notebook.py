"""The exploration notebook is documentation that can rot silently, so it is
executed like any other test. Skipped when the notebook extra isn't installed."""

from pathlib import Path

import pytest

nbformat = pytest.importorskip("nbformat")
nbclient = pytest.importorskip("nbclient")

NOTEBOOK = Path(__file__).parent.parent / "notebooks" / "explore.ipynb"


def test_notebook_has_no_committed_outputs():
    """Outputs in git make every run a diff, and go stale without warning."""
    nb = nbformat.read(NOTEBOOK, as_version=4)
    with_output = [
        i for i, c in enumerate(nb.cells) if c.cell_type == "code" and c.get("outputs")
    ]
    assert not with_output, f"clear outputs before committing (cells {with_output})"


@pytest.mark.slow
def test_notebook_runs_top_to_bottom():
    from nbclient import NotebookClient

    nb = nbformat.read(NOTEBOOK, as_version=4)
    NotebookClient(
        nb,
        timeout=300,
        kernel_name="python3",
        resources={"metadata": {"path": str(NOTEBOOK.parent)}},
    ).execute()
