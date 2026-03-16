"""
Lakebase PostgreSQL connection pool via asyncpg.
Conecta ao database crm_app no Lakebase onde as synced tables do Gold residem.
"""

import os
import asyncpg


class LakebasePool:
    def __init__(self):
        self._pool = None

    async def init(self):
        if self._pool:
            return
        self._pool = await asyncpg.create_pool(
            host=os.environ["LAKEBASE_HOST"],
            port=5432,
            database=os.environ["LAKEBASE_DB"],
            user=os.environ["LAKEBASE_USER"],
            password=os.environ["LAKEBASE_PASSWORD"],
            ssl="require",
            min_size=2,
            max_size=10,
        )
        print(f"Lakebase pool initialized: {os.environ['LAKEBASE_HOST']}/{os.environ['LAKEBASE_DB']}")

    async def fetch(self, query: str, *args):
        async with self._pool.acquire() as conn:
            await conn.execute("SET search_path TO crm_app, public")
            rows = await conn.fetch(query, *args)
            return [dict(r) for r in rows]

    async def fetchrow(self, query: str, *args):
        async with self._pool.acquire() as conn:
            await conn.execute("SET search_path TO crm_app, public")
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None

    async def close(self):
        if self._pool:
            await self._pool.close()


pool = LakebasePool()
