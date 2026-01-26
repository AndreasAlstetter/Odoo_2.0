import xmlrpc.client
import logging
from typing import Any, Dict, List, Optional

from .config import OdooConfig

logger = logging.getLogger(__name__)

class OdooClient:
    def __init__(self, config: Optional[OdooConfig] = None) -> None:
        self.config = config or OdooConfig.from_env()
        self._uid: Optional[int] = None
        self._common = xmlrpc.client.ServerProxy(f"{self.config.url}/xmlrpc/2/common")
        self._models = xmlrpc.client.ServerProxy(f"{self.config.url}/xmlrpc/2/object")
        self._ref_cache = {}
        self._defaults = {
            'uom_unit': 1,
            'product_category': 1,
            'company': 1,
            'fifo_strategy': False
        }

    @property
    def uid(self) -> int:
        if self._uid is None:
            self._uid = self._common.authenticate(
                self.config.db, self.config.user, self.config.password, {}
            )
            if not self._uid:
                raise RuntimeError(f"Odoo Auth failed: {self.config.db}/{self.config.user}")
        return self._uid

    @property
    def db(self) -> str:
        return self.config.db

    @property
    def password(self) -> str:
        return self.config.password

    def _safe_call(self, model: str, method: str, args: List[Any], kwargs: Optional[Dict] = None) -> Any:
        """🔧 FIXED: kwargs handling für search/read (limit/fields)."""
        clean_args = [[v for v in arg if v is not None] if isinstance(arg, list) else arg for arg in args]
        clean_kwargs = kwargs or {}
        clean_kwargs = {k: v for k, v in clean_kwargs.items() if v is not None}
        try:
            return self._models.execute_kw(self.db, self.uid, self.password, model, method, clean_args, clean_kwargs)
        except Exception as e:
            logger.error(f"[{model}.{method}] {str(e)[:150]}")
            raise

    def search(self, model: str, domain: List[Any], limit: Optional[int] = None) -> List[int]:
        """✅ search(domain, limit=1) - positional + kwargs OK."""
        kwargs = {'limit': limit} if limit else {}
        return self._safe_call(model, 'search', [domain], kwargs)

    def search_read(self, model: str, domain: List[Any], fields: List[str], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """🔥 NEW: search_read(domain, fields, limit) - für Kanban FlowRack."""
        kwargs = {'fields': fields, 'limit': limit}
        return self._safe_call(model, 'search_read', [domain], kwargs)

    def create(self, model: str, vals: Dict[str, Any]) -> int:
        """✅ create(vals) - cleaned vals."""
        clean_vals = {k: v for k, v in vals.items() if v is not None}
        for bad_field in ['detailed_type', 'product_tmpl_id']:
            clean_vals.pop(bad_field, None)
        return self._safe_call(model, 'create', [clean_vals])

    def write(self, model: str, ids: List[int], vals: Dict[str, Any]) -> bool:
        clean_vals = {k: v for k, v in vals.items() if v is not None}
        clean_vals.pop('detailed_type', None)
        return self._safe_call(model, 'write', [ids, clean_vals])

    def read(self, model: str, ids: List[int], fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        kwargs = {'fields': fields} if fields else {}
        return self._safe_call(model, 'read', [ids], kwargs)

    def unlink(self, model: str, ids: List[int]) -> bool:
        return self._safe_call(model, 'unlink', [ids])

    def ref(self, xml_id: str, cache: bool = True) -> Optional[int]:
        if cache and xml_id in self._ref_cache:
            return self._ref_cache[xml_id]
        try:
            res_id = self._safe_call('ir.model.data', 'xmlid_to_res_id', [xml_id])
            if cache:
                self._ref_cache[xml_id] = res_id
            return res_id
        except:
            if cache:
                self._ref_cache[xml_id] = None
            return None

    # 🔥 v6.0 Helpers (wie in KLTLoader v7.0 verwendet)
    def get_uom_unit(self) -> int:
        return self.ref('uom.product_uom_unit') or self._defaults['uom_unit']

    def get_product_category(self) -> int:
        return self.ref('product.product_category_all') or self._defaults['product_category']

    def get_company_id(self) -> int:
        if self._defaults['company'] != 1:
            return self._defaults['company']
        try:
            companies = self.read('res.users', [self.uid], ['company_id'])
            cid = companies[0].get('company_id', [False, 1])[0] or 1
            self._defaults['company'] = cid
            return cid
        except:
            return 1

    def get_fifo_strategy(self) -> Optional[int]:
        return self.ref('stock.removal_fifo') or self._defaults['fifo_strategy']

    def ensure_record(self, *args, **kwargs) -> int:
        """🔥 v6.2: Handle alt: ensure_record(domain, create_vals=vals)"""
        if len(args) == 2 and 'create_vals' in kwargs:
            model, domain = args[0], args[1]  # Alte Loaders: ensure_record(model, domain, create_vals=vals)
            vals = kwargs['create_vals']
        elif len(args) == 3:
            model, domain, vals = args  # Neue Syntax
        else:
            raise ValueError("ensure_record(model, domain, vals) or ensure_record(model, domain, create_vals=vals)")
        
        ids = self.search(model, domain, limit=1)
        if ids:
            return ids[0]
        return self.create(model, vals)

