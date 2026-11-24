from psycopg_pool import AsyncConnectionPool

from .config import settings

# opened in lifespan, not here. otherwise importing this in tests tries to hit postgres
pool = AsyncConnectionPool(settings.database_url, min_size=1, max_size=10, open=False)
