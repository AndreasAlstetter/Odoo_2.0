import xmlrpc.client
from typing import Any, Dict, List, Optional, Tuple

from .config import OdooConfig

class OdooClient:
    def __init__(self, config: Optional[OdooConfig] = None) -> None:
        self.config = config or OdooConfig.from_env()
        self._uid: Optional[int] = None
        self._common = xmlrpc.client.ServerProxy(f"{self.config.url}/xmlrpc/2/common")
        self._models = xmlrpc.client.ServerProxy(f"{self.config.url}/xmlrpc/2/object")

    @property
    def uid(self) -> int:
        if self._uid is None:
            self._uid = self._common.authenticate(
                self.config.db,
                self.config.user,
                self.config.password,
                {},
            )
            if not self._uid:
                raise RuntimeError(
                    f"Odoo Authentication failed: "
                    f"DB={self.config.db}, User={self.config.user}, "
                    f"URL={self.config.url}"
                )
        return self._uid

    @property
    def base_data_dir(self) -> str:
        """Kompatibilität für Loader: config.base_data_dir."""
        return self.config.base_data_dir or "./data"

    @property
    def models(self):
        """Expose models proxy für direkte execute_kw calls."""
        return self._models

    @property
    def db(self) -> str:
        """Expose DB name."""
        return self.config.db

    @property
    def password(self) -> str:
        """Expose password."""
        return self.config.password

    def call(self, model: str, method: str, args, **kwargs) -> Any:
        """
        Low-Level-Wrapper um execute_kw.

        args: Liste der Positionsargumente für Odoo, z. B.
              [domain], [ids, fields], [vals], ...
        """
        return self._models.execute_kw(
            self.config.db,
            self.uid,
            self.config.password,
            model,
            method,
            args,
            kwargs,
        )

    # Convenience-Methoden
    def search(self, model: str, domain: List, limit: Optional[int] = None) -> List[int]:
        kwargs: Dict[str, Any] = {}
        if limit:
            kwargs["limit"] = limit
        return self.call(model, "search", [domain], **kwargs)

    def search_read(
        self,
        model: str,
        domain: List,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        kwargs: Dict[str, Any] = {}
        if fields:
            kwargs["fields"] = fields
        if limit:
            kwargs["limit"] = limit
        return self.call(model, "search_read", [domain], **kwargs)

    def read(self, model: str, ids: List[int], fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        🚀 v4.1.1 ADDED: Read specific fields from records.
        
        Args:
            model: Odoo model name (e.g. 'stock.rule')
            ids: List of record IDs
            fields: Optional list of field names to retrieve
            
        Returns:
            List of dicts with requested fields
        """
        kwargs: Dict[str, Any] = {}
        if fields:
            kwargs["fields"] = fields
        return self.call(model, "read", [ids], **kwargs)

    def create(self, model: str, vals: Dict[str, Any]) -> int:
        return self.call(model, "create", [vals])

    def write(self, model: str, ids: List[int], vals: Dict[str, Any]) -> bool:
        return self.call(model, "write", [ids, vals])

    def unlink(self, model: str, ids: List[int]) -> bool:
        return self.call(model, "unlink", [ids])

    def ensure_record(
        self,
        model: str,
        domain: List,
        create_vals: Dict[str, Any],
        update_vals: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, bool]:
        """Erstelle Record oder update existierenden (idempotent)."""
        ids = self.search(model, domain, limit=1)
        if ids:
            if update_vals is not None:  # FIX: None-Check
                self.write(model, ids, update_vals)
            return ids[0], False

        rec_id = self.create(model, create_vals)
        return rec_id, True
