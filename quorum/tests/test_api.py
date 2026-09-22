"""tests/test_api.py
-------------------
Unit tests for FastAPI endpoints:
- Health check
- Runs CRUD and erasure
- Bus sense/claim/finding
- Conflicts listing and human resolution
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from api.main import app
from api.auth import create_access_token
from db.models import init_db


@pytest_asyncio.fixture(autouse=True)
async def setup_database():
    """Ensure database tables exist before API tests run."""
    await init_db()


@pytest.fixture
def auth_headers():
    token = create_access_token(data={"sub": "testuser"})
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_health_check():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok", "service": "quorum"}


@pytest.mark.asyncio
async def test_runs_lifecycle(auth_headers):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Create run
        create_resp = await ac.post(
            "/runs/",
            json={"question": "What is Quorum?", "participant_ids": ["agent-1"]},
            headers=auth_headers,
        )
        assert create_resp.status_code == 201
        run_data = create_resp.json()
        run_id = run_data["id"]

        # 2. Get run
        get_resp = await ac.get(f"/runs/{run_id}", headers=auth_headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["id"] == run_id

        # 3. Delete run (full erasure)
        del_resp = await ac.delete(f"/runs/{run_id}", headers=auth_headers)
        assert del_resp.status_code == 200
        assert del_resp.json()["run_id"] == run_id


@pytest.mark.asyncio
async def test_bus_sense_and_claim(auth_headers):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Claim
        claim_resp = await ac.post(
            "/bus/claim",
            json={
                "run_id": "test-bus-run",
                "participant_id": "agent-1",
                "participant_type": "agent",
                "content": "API test claim content",
            },
            headers=auth_headers,
        )
        assert claim_resp.status_code == 201

        # Sense
        sense_resp = await ac.post(
            "/bus/sense",
            json={"query": "test claim content", "namespace": "claims", "n_results": 5},
            headers=auth_headers,
        )
        assert sense_resp.status_code == 200
        data = sense_resp.json()
        assert "results" in data
        assert "latency_ms" in data


@pytest.mark.asyncio
async def test_atomic_claim_deduplication():
    """Verify claim(..., atomic=True) prevents concurrent duplicate claims under _claims_lock."""
    from bus.moss_client import claim

    c1, is_dup1 = await claim(
        run_id="run-atomic-test",
        participant_id="agent-1",
        participant_type="agent",
        content="Decentralized consensus mechanisms in multi-agent swarms",
        atomic=True,
    )
    assert is_dup1 is False

    # Second near-identical claim should be detected as duplicate within lock
    c2, is_dup2 = await claim(
        run_id="run-atomic-test",
        participant_id="agent-2",
        participant_type="agent",
        content="Decentralized consensus mechanisms in multi-agent swarms",
        atomic=True,
    )
    assert is_dup2 is True
    assert c2.id == c1.id

