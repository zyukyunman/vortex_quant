from __future__ import annotations

from vortex.research.cogalpha import AlphaCandidate, LineageRecord
from vortex.research.cogalpha.agent_catalog import agent_by_name, is_registered_agent, registered_agents


def test_alpha_candidate_serializes_lineage_and_metadata():
    candidate = AlphaCandidate(
        alpha_id="vtx_cogalpha_0001",
        name="liquidity_impact_reversal_20d",
        agent="AgentLiquidity",
        hypothesis="Large range under thin liquidity may predict a premium.",
        expression="cs_rank((high - low) / amount)",
        required_fields=("high", "low", "amount"),
        lookback_windows=(20,),
        horizons=(1, 5, 20),
        lineage=LineageRecord(generation=1, parents=("seed_alpha",), mutation_type="operator_swap"),
        metadata={"source": "unit_test"},
    )

    payload = candidate.to_dict()

    assert payload["alpha_id"] == "vtx_cogalpha_0001"
    assert payload["lineage"]["parents"] == ["seed_alpha"]
    assert payload["required_fields"] == ["high", "low", "amount"]
    assert payload["metadata"] == {"source": "unit_test"}


def test_agent_catalog_registers_twenty_one_agents():
    agents = registered_agents()

    assert len(agents) == 21
    assert len({agent.name for agent in agents}) == 21
    assert is_registered_agent("AgentLiquidity")
    assert agent_by_name("AgentLiquidity").layer == "Price-Volume Dynamics"
