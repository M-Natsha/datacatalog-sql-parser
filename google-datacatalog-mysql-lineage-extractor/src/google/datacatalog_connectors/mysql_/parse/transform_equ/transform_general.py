from google.datacatalog_connectors.mysql_.parse.transform_equ \
    .transform_create import TransformCreate
from google.datacatalog_connectors.mysql_.parse.transform_equ \
    .transform_delete import TransformDelete
from google.datacatalog_connectors.mysql_.parse.transform_equ \
    .transform_update import TransformUpdate


class TransformGeneral:
    _transformer = None

    def can_transform(self, query: str) -> bool:
        return True

    def transform(self, query: str) -> str:

        query = query.replace("\"", "\'")
        delete_tranform = TransformDelete()
        update_transform = TransformUpdate()
        create_transform = TransformCreate()

        if (delete_tranform.can_transform(query)):
            self._transformer = delete_tranform
        elif (update_transform.can_transform(query)):
            self._transformer = update_transform
        elif (create_transform.can_transform(query)):
            self._transformer = create_transform

        if self._transformer is not None:
            return self._transformer.transform(query)
        else:
            return query

    def post_parse_transform(self, query):
        if self._transformer is not None:
            return self._transformer.post_parse_transform(query)

        return query
