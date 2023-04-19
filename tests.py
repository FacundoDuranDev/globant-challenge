import pytest
import requests

@pytest.fixture
def client():
    from app import app
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_invalid_schema(client):
    res = client.post('/example', data={'filename': 'data.csv', 'schema': 'invalid', 'batch': '500'})
    assert res.status_code == 400
    assert res.data == b'invalid schema'

def test_invalid_batch_size(client):
    res = client.post('/example', data={'filename': 'data.csv', 'schema': 'departments', 'batch': '5000'})
    assert res.status_code == 400
    assert res.data == b'invalid batch size, maximum 1000'