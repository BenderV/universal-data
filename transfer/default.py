import sqlalchemy as sa
from loguru import logger


class DefaultLoader:
    def __init__(self, source_uri: sa.engine.Engine, dest_uri: str, table_name: str):
        self.source_engine = sa.create_engine(source_uri, pool_pre_ping=True)
        self.dest_engine = sa.create_engine(dest_uri, pool_pre_ping=True)
        self.table_name = table_name

    @property
    def source_table(self):
        return sa.Table(
            self.table_name,
            sa.MetaData(),
            autoload=True,
            autoload_with=self.source_engine,
        )

    @property
    def dest_table(self):
        raise NotImplementedError

    def _select_added_rows(self):
        """
        Select rows from table, and filter rows
        with __hash key not in the Transfer table with hash, table_name & target_id
        """
        results = self.source_engine.execute(
            f"""
            SELECT * FROM "{self.table_name}"
            WHERE __hash NOT IN (
                SELECT hash
                FROM __ud_transfer
                WHERE table_name = '{self.table_name}'
                AND target_id = 1
            )
            """
        )
        rows = [dict(r) for r in results.fetchall()]
        logger.info(f"new: {len(rows)} rows")
        return rows

    def _select_removed_hashs(self):
        """
        Select __hash from table Transfer which are not anymore in the source table
        """
        results = self.source_engine.execute(
            f"""
            SELECT hash
            FROM __ud_transfer
            WHERE table_name = '{self.table_name}'
            AND target_id = 1
            AND hash NOT IN (
                SELECT __hash FROM "{self.table_name}"
            )
            """
        )
        hashs = [r["hash"] for r in results.fetchall()]
        logger.info(f"removed: {len(hashs)} rows")
        return hashs

    def _reset_transfered_rows(self):
        self.source_engine.execute(
            f"DELETE FROM __ud_transfer WHERE table_name = '{self.table_name}' AND target_id = 1"
        )

    def _remove_transfered_hashs(self, hashs):
        self.source_engine.execute(
            f"""
            DELETE FROM __ud_transfer
            WHERE table_name = '{self.table_name}'
            AND target_id = 1
            AND hash IN ({', '.join([f"'{h}'" for h in hashs])})
            """
        )

    def _insert_transfered_rows(self, rows):
        self.source_engine.execute(
            f"""
            INSERT INTO __ud_transfer (table_name, hash, target_id, created_at)
            VALUES {','.join([f"('{self.table_name}', '{r['__hash']}', 1, NOW())" for r in rows])}
            """
        )

    def support_uri(self):
        raise NotImplementedError

    def _check_table_have_same_columns(self):
        raise NotImplementedError

    def _upload(self, _):
        raise NotImplementedError

    def _remove(self, _):
        raise NotImplementedError

    def _create_table_from_source(self):
        raise NotImplementedError

    def _table_exist(self):
        insp = sa.inspect(self.dest_engine)
        table_exist = insp.has_table(self.table_name)
        if not table_exist:
            logger.warning(f"Table {self.table_name} does not exist on destination")
        return table_exist

    def transfer(self):
        if (
            self._table_exist() is False
            or self._check_table_have_same_columns() is False
        ):
            # Recreate table
            self._create_table_from_source()
            # Empty __ud_transfer
            self._reset_transfered_rows()

        rows = self._select_added_rows()
        if rows:
            self._upload(rows)
            self._insert_transfered_rows(rows)

        hashs = self._select_removed_hashs()
        if hashs:
            self._remove(hashs)
            self._remove_transfered_hashs(hashs)
