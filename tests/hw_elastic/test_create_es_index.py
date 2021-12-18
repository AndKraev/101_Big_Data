from unittest.mock import Mock

from pytest import fixture

from hw_elastic.create_es_index import create_es_index


@fixture
def mocked_es():
    return Mock()


def test_create_es_index_index_creation(mocked_es):
    create_es_index(mocked_es, "name", {"test": 1})
    mocked_es.indices.create.assert_called_once_with(index="name", body={"test": 1})


def test_create_es_index_deletion(mocked_es):
    create_es_index(mocked_es, "name", {"test": 1}, delete=True)
    mocked_es.indices.exists.assert_called_once_with(index="name")
    mocked_es.indices.delete.assert_called_once_with(index="name")


def test_create_es_not_deleted_with_delete_false(mocked_es):
    create_es_index(mocked_es, "name", {"test": 1}, delete=False)
    mocked_es.indices.delete.assert_not_called()


def test_create_es_index_not_deleted_if_index_doesnt_exist(mocked_es):
    mocked_es.indices.exists.return_value = False
    create_es_index(mocked_es, "name", {"test": 1}, delete=True)
    mocked_es.indices.exists.assert_called_once_with(index="name")
    mocked_es.indices.delete.assert_not_called()
