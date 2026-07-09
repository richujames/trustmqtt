import datetime

from tmq_worker.storage import (
    Client, FeatureWindowRow, get_engine, get_or_create_client,
    get_sessionmaker, init_db,
)


def make_session():
    engine = get_engine("sqlite:///:memory:")
    init_db(engine)
    return get_sessionmaker(engine)()


def test_get_or_create_client_is_idempotent():
    session = make_session()
    c1 = get_or_create_client(session, "dev-1", username="plant-a")
    session.commit()
    c2 = get_or_create_client(session, "dev-1")
    assert c1.id == c2.id
    assert session.query(Client).count() == 1


def test_feature_window_row_roundtrip_with_jsonb_variant_field():
    session = make_session()
    client = get_or_create_client(session, "dev-1")
    session.commit()

    row = FeatureWindowRow(
        client_id=client.id,
        window_start=datetime.datetime(2026, 7, 7, 0, 0, 0),
        window_len_s=60.0,
        features={"msg_rate": 1.2, "unique_topics": 3},
        fsm_violation=0.1,
        drift=0.2,
        fleet=0.0,
        trust=0.15,
    )
    session.add(row)
    session.commit()

    fetched = session.query(FeatureWindowRow).first()
    assert fetched.features["msg_rate"] == 1.2
    assert fetched.trust == 0.15
