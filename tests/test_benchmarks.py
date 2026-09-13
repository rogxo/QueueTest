"""Protect measurement validity; performance thresholds are deliberately absent."""

import subprocess

import pytest

pytest.importorskip("psutil", reason="Install .[benchmark] to validate benchmark tooling")

from benchmarks.evaluate import (  # noqa: E402
    Scenario,
    latency_summary,
    percentile,
    run_child,
    transfer,
    verify_delivery,
)


def test_nearest_rank_percentiles_and_empty_samples():
    assert percentile([], 99) is None
    assert percentile([40, 10, 20, 30], 50) == 20
    assert percentile([40, 10, 20, 30], 99) == 40
    assert latency_summary([])["samples"] == 0


@pytest.mark.parametrize("batches", [[[0, 1, 1]], [[0, 1]], [[0, 1, 3]], [[2, 0, 1]]])
def test_delivery_checker_rejects_invalid_histories(batches):
    with pytest.raises(AssertionError):
        verify_delivery(batches, 3, 1)


def test_delivery_checker_allows_interleaving_consumers():
    verify_delivery([[0, 2, 1], [3, 4, 5]], 6, 2)


@pytest.mark.parametrize("queue_name", ["LockFreeQueue", "SimpleQueue", "Queue"])
@pytest.mark.parametrize("mode", ["live", "fill", "drain"])
def test_transfer_modes_validate_messages(queue_name, mode):
    scenario = Scenario(
        "test",
        producers=0 if mode == "drain" else 2,
        consumers=0 if mode == "fill" else 2,
        mode=mode,
    )
    result = transfer(queue_name, scenario, 32)
    assert result["delivery_verified"]
    assert result["messages"] == 32
    assert result["seconds"] > 0


def test_latency_samples_cover_every_producer():
    result = transfer("LockFreeQueue", Scenario("latency", sample_every=4), 64)
    assert result["put_latency"]["samples"] == 16
    assert result["end_to_end_latency"]["samples"] == 16
    assert result["end_to_end_latency"]["p99_us"] > 0


def test_child_process_failures_are_not_reported_as_measurements():
    with pytest.raises(subprocess.CalledProcessError):
        run_child({"kind": "invalid"}, timeout=10)


def test_child_has_a_hard_timeout():
    with pytest.raises(subprocess.TimeoutExpired):
        run_child({"kind": "primitives", "iterations": 10**9}, timeout=0.1)


def test_report_rejects_unfinished_or_failed_measurements():
    from benchmarks.report import summarize

    with pytest.raises(ValueError, match="incomplete"):
        summarize({"metadata": {}, "results": []})
    for verified in (None, False):
        with pytest.raises(ValueError, match="validation failed"):
            summarize(
                {
                    "metadata": {"completed_utc": "test"},
                    "results": [{"kind": "transfer", "delivery_verified": verified}],
                }
            )


def test_report_requires_all_scenarios():
    from benchmarks.report import require_full_suite

    with pytest.raises(ValueError, match="full scenario set"):
        require_full_suite({"metadata": {"settings": {"rounds": 3}}}, {"transfers": []})
