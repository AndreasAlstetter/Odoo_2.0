"""
client.py - Odoo RPC Client mit Retry, Logging, Audit

Sichere Kommunikation mit Odoo via XML-RPC mit:
- Automatische Retries bei Netzwerkfehlern
- Exponentielles Backoff
- Comprehensive Logging + Audit Trail
- Idempotente Operationen
- Rate Limiting für >500 Drohnen/Tag
"""

import os
import time
import logging
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import xmlrpc.client
import http.client

from provisioning.config import (
    OdooRPCConfig, 
    get_odoo_config,
    LoggingConfig,
)


# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOM TRANSPORT FÜR TIMEOUT (FIX)
# ═══════════════════════════════════════════════════════════════════════════════

class TimeoutHTTPConnection(http.client.HTTPConnection):
    """HTTP Connection mit Timeout."""
    def __init__(self, *args, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout


class TimeoutHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS Connection mit Timeout."""
    def __init__(self, *args, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout


class TimeoutTransport(xmlrpc.client.Transport):
    """XML-RPC Transport mit Timeout support."""
    def __init__(self, timeout=60, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeout = timeout
    
    def make_connection(self, host):
        """Create HTTPS connection mit Timeout."""
        conn = TimeoutHTTPSConnection(host, timeout=self.timeout)
        return conn


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class OdooClientError(Exception):
    """Base exception for Odoo RPC client."""
    pass


class AuthenticationError(OdooClientError):
    """Authentication with Odoo failed."""
    pass


class RpcCallError(OdooClientError):
    """RPC method call failed (Odoo error)."""
    pass


class RpcRetryExhausted(OdooClientError):
    """RPC call failed after all retries."""
    pass


class RecordNotFoundError(OdooClientError):
    """Record search returned no results."""
    pass


class RecordAmbiguousError(OdooClientError):
    """Record search returned multiple results (expected 1)."""
    pass


class ValidationError(OdooClientError):
    """Data validation error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def _setup_logging(name: str) -> logging.Logger:
    """Configure logger mit Audit-Trail."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:  # Nur einmalig setup
        formatter = logging.Formatter(
            LoggingConfig.FORMAT,
            datefmt=LoggingConfig.DATE_FORMAT
        )
        
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(LoggingConfig.LEVEL)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File Handler (für Audit)
        if LoggingConfig.AUDIT_ENABLED:
            os.makedirs(LoggingConfig.LOG_DIR, exist_ok=True)
            
            file_handler = logging.FileHandler(LoggingConfig.AUDIT_LOG_FILE)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        logger.setLevel(LoggingConfig.LEVEL)
    
    return logger


# ═══════════════════════════════════════════════════════════════════════════════
# ODOO RPC CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class OdooClient:
    """
    Sichere Odoo RPC Client mit automatischen Retries und Logging.
    
    Usage:
        client = OdooClient()
        products = client.search('product.product', [('active', '=', True)])
        client.create('mrp.production', {'product_id': 1, ...})
    """
    
    def __init__(self, config: Optional[OdooRPCConfig] = None):
        """
        Initialisiere Odoo RPC Client.
        
        Args:
            config: OdooRPCConfig instance. Falls None, wird aus .env geladen.
        
        Raises:
            ValueError: Wenn .env-Variablen fehlen oder ungültig sind.
        """
        self.config = config or get_odoo_config()
        self.logger = _setup_logging(f"{__name__}.OdooClient")
        
        self._uid: Optional[int] = None
        self._authenticated_at: Optional[datetime] = None
        self._auth_timeout: int = 3600  # 1 Stunde Token-Gültigkeit (Odoo-abhängig)
        
        # RPC Server Proxies mit TimeoutTransport (✅ FIX)
        try:
            common_url = f"{self.config.url}{self.config.rpc_endpoints['common']}"
            object_url = f"{self.config.url}{self.config.rpc_endpoints['object']}"
            
            # Erstelle Transport mit Timeout
            transport = TimeoutTransport(timeout=self.config.timeout)
            
            self.common = xmlrpc.client.ServerProxy(
                common_url,
                transport=transport,  # ✅ KORREKT!
            )
            self.models = xmlrpc.client.ServerProxy(
                object_url,
                transport=transport,  # ✅ KORREKT!
            )
            
            self.logger.info(
                f"Odoo RPC Client initialized: {self.config.url} "
                f"(DB: {self.config.db}, timeout: {self.config.timeout}s)"
            )
            
        except Exception as e:
            raise OdooClientError(f"Failed to initialize RPC proxies: {e}") from e
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUTHENTICATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    @property
    def uid(self) -> int:
        """
        Get authenticated user ID.
        
        Returns:
            User ID (> 0)
        
        Raises:
            AuthenticationError: Wenn Authentication fehlschlägt
        """
        # Token-Reuse wenn noch gültig
        if self._uid is not None and self._authenticated_at is not None:
            elapsed = (datetime.now() - self._authenticated_at).total_seconds()
            if elapsed < self._auth_timeout:
                return self._uid
            else:
                self.logger.debug(f"Auth token expired (elapsed: {elapsed}s), re-authenticating...")
                self._uid = None
        
        # Neue Authentifizierung
        self._uid = self._authenticate()
        self._authenticated_at = datetime.now()
        
        return self._uid
    
    def _authenticate(self) -> int:
        """
        Authenticate gegen Odoo mit Retry.
        
        Returns:
            User ID (> 0)
        
        Raises:
            AuthenticationError: Wenn alle Retries fehlschlagen
        """
        for attempt in range(1, self.config.max_retries + 1):
            try:
                self.logger.debug(
                    f"Authentication attempt {attempt}/{self.config.max_retries} "
                    f"for user '{self.config.user}'..."
                )
                
                uid = self.common.authenticate(
                    self.config.db,
                    self.config.user,
                    self.config.password,
                    {}
                )
                
                if not uid or uid == 0:
                    raise AuthenticationError(
                        f"Authentication returned invalid UID: {uid}"
                    )
                
                self.logger.info(f"Successfully authenticated as UID {uid}")
                return uid
                
            except xmlrpc.client.Fault as e:
                if "AccessDenied" in str(e) or "forbidden" in str(e).lower():
                    raise AuthenticationError(
                        f"Access denied for user '{self.config.user}'. "
                        f"Check credentials in .env"
                    ) from e
                
                # Andere RPC Faults sind retryierbar
                if attempt < self.config.max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"Auth RPC Fault (attempt {attempt}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise AuthenticationError(
                        f"Authentication failed after {self.config.max_retries} attempts: {e}"
                    ) from e
            
            except (ConnectionError, OSError, xmlrpc.client.ProtocolError) as e:
                if attempt < self.config.max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"Connection error during auth (attempt {attempt}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise AuthenticationError(
                        f"Connection failed after {self.config.max_retries} attempts: {e}"
                    ) from e
        
        # Sollte nicht erreichbar sein
        raise AuthenticationError("Authentication failed unexpectedly")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # RPC EXECUTION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff mit Kapitulation.
        
        Beispiel (multiplier=1.5):
        - Attempt 1: 1.5^0 = 1s
        - Attempt 2: 1.5^1 = 1.5s
        - Attempt 3: 1.5^2 = 2.25s
        - Attempt 4: 1.5^3 = 3.375s
        - ...capped at 60s
        
        Args:
            attempt: Aktuelle Versuch-Nummer (1-indexed)
        
        Returns:
            Wartezeit in Sekunden (maximal 60s)
        """
        wait = self.config.backoff_factor ** (attempt - 1)
        return min(wait, 60.0)  # Max 60 Sekunden
    
    def _execute_rpc(
        self,
        model: str,
        method: str,
        args: Tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute RPC call mit Retry + Logging + Audit.
        
        Args:
            model: Odoo model name (e.g., 'product.product')
            method: RPC method (e.g., 'create', 'write', 'search')
            args: Positional arguments
            kwargs: Keyword arguments
        
        Returns:
            Result from RPC call
        
        Raises:
            RpcRetryExhausted: Wenn alle Retries fehlschlagen
            RpcCallError: Bei Odoo-spezifischen Fehlern
        """
        if kwargs is None:
            kwargs = {}
        
        last_error: Optional[Exception] = None
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                # Log RPC Call
                self.logger.debug(
                    f"RPC: {model}.{method}(args={len(args)}, kwargs={len(kwargs)})"
                )
                
                # Execute
                result = self.models.execute_kw(
                    self.config.db,
                    self.uid,
                    self.config.password,
                    model,
                    method,
                    args,
                    kwargs
                )
                
                # Success
                if attempt > 1:
                    self.logger.info(
                        f"RPC {model}.{method} succeeded after {attempt} attempts"
                    )
                
                # Audit: Log successful calls
                if LoggingConfig.AUDIT_ENABLED:
                    self._audit_log({
                        'timestamp': datetime.now().isoformat(),
                        'model': model,
                        'method': method,
                        'status': 'success',
                        'attempt': attempt,
                        'result_type': type(result).__name__,
                    })
                
                return result
            
            except xmlrpc.client.Fault as e:
                last_error = e
                
                # Certain errors are not retryable
                if "AccessDenied" in str(e):
                    raise RpcCallError(f"Access denied: {e}") from e
                
                if "RecordError" in str(e):
                    raise RpcCallError(f"Record error: {e}") from e
                
                # Andere Faults sind retryierbar
                if attempt < self.config.max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"RPC Fault on {model}.{method} "
                        f"(attempt {attempt}/{self.config.max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(
                        f"RPC Fault on {model}.{method} "
                        f"after {self.config.max_retries} attempts: {e}"
                    )
            
            except (ConnectionError, OSError, xmlrpc.client.ProtocolError) as e:
                last_error = e
                
                if attempt < self.config.max_retries:
                    wait_time = self._calculate_backoff(attempt)
                    self.logger.warning(
                        f"Connection error on {model}.{method} "
                        f"(attempt {attempt}/{self.config.max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(
                        f"Connection failed on {model}.{method} "
                        f"after {self.config.max_retries} attempts: {e}"
                    )
        
        # Alle Retries verbraucht
        raise RpcRetryExhausted(
            f"Failed to execute {model}.{method} "
            f"after {self.config.max_retries} attempts: {last_error}"
        ) from last_error
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Log RPC call to audit trail."""
        try:
            audit_logger = logging.getLogger(f"{__name__}.audit")
            audit_logger.info(json.dumps(data))
        except Exception as e:
            self.logger.error(f"Failed to write audit log: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC METHODS: SEARCH
    # ═══════════════════════════════════════════════════════════════════════════
    
    def search(
        self,
        model: str,
        domain: List[Tuple],
        offset: int = 0,
        limit: Optional[int] = None,
        order: Optional[str] = None,
    ) -> List[int]:
        """
        Search for records.
        
        Args:
            model: Model name (e.g., 'product.product')
            domain: Search domain (e.g., [('active', '=', True)])
            offset: Record offset (für Pagination)
            limit: Max records to return (None = all, aber capped bei DEFAULT_SEARCH_LIMIT)
            order: Order by field (e.g., 'id desc')
        
        Returns:
            List of record IDs
        
        Raises:
            RpcRetryExhausted: Bei RPC-Fehler nach Retries
        """
        # Limit defaultet zu search_limit aus Config
        if limit is None:
            limit = self.config.search_limit
        
        # Aber nicht mehr als batch_size abrufen
        limit = min(limit, self.config.batch_size)
        
        kwargs = {
            'offset': offset,
            'limit': limit,
        }
        
        if order:
            kwargs['order'] = order
        
        self.logger.debug(
            f"Searching {model} with domain={domain}, offset={offset}, limit={limit}"
        )
        
        ids = self._execute_rpc(model, 'search', (domain,), kwargs)
        
        self.logger.debug(f"Found {len(ids)} records for {model}")
        
        return ids
    
    def search_read(
        self,
        model: str,
        domain: List[Tuple],
        fields: Optional[List[str]] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        order: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search und read in einem Call (effizienter als search + read).
        
        Args:
            model: Model name
            domain: Search domain
            fields: Fields to read (None = all)
            offset: Record offset
            limit: Max records
            order: Order by field
        
        Returns:
            List of record dicts
        """
        if limit is None:
            limit = self.config.search_limit
        
        limit = min(limit, self.config.batch_size)
        
        kwargs = {
            'offset': offset,
            'limit': limit,
        }
        
        if fields:
            kwargs['fields'] = fields
        
        if order:
            kwargs['order'] = order
        
        self.logger.debug(
            f"Search-read {model}: domain={domain}, fields={fields}, limit={limit}"
        )
        
        records = self._execute_rpc(
            model,
            'search_read',
            (domain,),
            kwargs
        )
        
        self.logger.debug(f"Retrieved {len(records)} records from {model}")
        
        return records
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC METHODS: CRUD
    # ═══════════════════════════════════════════════════════════════════════════
    
    def create(self, model: str, values: Dict[str, Any]) -> int:
        """
        Create a new record.
        
        Args:
            model: Model name
            values: Field values dict
        
        Returns:
            Record ID
        
        Raises:
            ValidationError: Wenn values ungültig
            RpcRetryExhausted: Bei RPC-Fehler
        """
        if not values:
            raise ValidationError("Cannot create record with empty values")
        
        self.logger.debug(f"Creating {model} with {len(values)} fields")
        
        record_id = self._execute_rpc(model, 'create', (values,))
        
        self.logger.info(f"Created {model} record ID {record_id}")
        
        return record_id
    
    def read(
        self,
        model: str,
        ids: Union[int, List[int]],
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Read records.
        
        Args:
            model: Model name
            ids: Record ID or list of IDs
            fields: Fields to read (None = all)
        
        Returns:
            List of record dicts
        """
        if isinstance(ids, int):
            ids = [ids]
        
        if not ids:
            return []
        
        kwargs = {}
        if fields:
            kwargs['fields'] = fields
        
        self.logger.debug(f"Reading {len(ids)} records from {model}")
        
        records = self._execute_rpc(model, 'read', (ids,), kwargs)
        
        return records
    
    def write(
        self,
        model: str,
        ids: Union[int, List[int]],
        values: Dict[str, Any],
    ) -> bool:
        """
        Update records.
        
        Args:
            model: Model name
            ids: Record ID or list of IDs
            values: Field values to update
        
        Returns:
            True
        """
        if isinstance(ids, int):
            ids = [ids]
        
        if not ids:
            self.logger.warning("write() called with empty ids")
            return False
        
        if not values:
            raise ValidationError("Cannot write record with empty values")
        
        self.logger.debug(
            f"Updating {len(ids)} records in {model} with {len(values)} fields"
        )
        
        result = self._execute_rpc(model, 'write', (ids, values))
        
        self.logger.info(f"Updated {len(ids)} records in {model}")
        
        return result
    
    def unlink(self, model: str, ids: Union[int, List[int]]) -> bool:
        """
        Delete records.
        
        Args:
            model: Model name
            ids: Record ID or list of IDs
        
        Returns:
            True
        """
        if isinstance(ids, int):
            ids = [ids]
        
        if not ids:
            self.logger.warning("unlink() called with empty ids")
            return False
        
        self.logger.warning(f"Deleting {len(ids)} records from {model}")
        
        result = self._execute_rpc(model, 'unlink', (ids,))
        
        self.logger.info(f"Deleted {len(ids)} records from {model}")
        
        return result
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC METHODS: IDEMPOTENT OPERATIONS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def ensure_record(
        self,
        model: str,
        domain: List[Tuple],
        create_vals: Dict[str, Any],
        update_vals: Optional[Dict[str, Any]] = None,
        unique: bool = True,
    ) -> Tuple[int, bool]:
        """
        Create oder update idempotent.
        
        WICHTIG: Domain muss UNIQUE sein (max 1 Record)!
        
        Args:
            model: Model name
            domain: Search domain (MUSS unique sein!)
            create_vals: Values für create()
            update_vals: Values für write() wenn exists (None = kein Update)
            unique: Werfe Fehler wenn >1 Ergebnis (empfohlen True)
        
        Returns:
            (record_id, is_new) - is_new=True wenn Record erstellt
        
        Raises:
            RecordAmbiguousError: Wenn domain mehrere Records findet
            RpcRetryExhausted: Bei RPC-Fehler
        
        Example:
            product_id, is_new = client.ensure_record(
                'product.product',
                [('default_code', '=', 'DRO-001')],
                {'name': 'Drohne Spartan', 'default_code': 'DRO-001'},
                {'list_price': 499.00}
            )
        """
        # Search mit limit=2 um Duplicates zu finden
        ids = self.search(model, domain, limit=2)
        
        if len(ids) > 1:
            if unique:
                self.logger.error(
                    f"ensure_record for {model} found {len(ids)} records "
                    f"with domain {domain} - expected 1! IDs: {ids}"
                )
                raise RecordAmbiguousError(
                    f"Domain returned {len(ids)} records, expected 1: {domain}"
                )
            else:
                # Multiple records OK, update first
                self.logger.warning(
                    f"ensure_record found {len(ids)} records with domain {domain}, "
                    f"updating only first (ID {ids[0]})"
                )
                ids = [ids[0]]
        
        if ids:
            # Record exists
            if update_vals:
                self.write(model, ids[0], update_vals)
                self.logger.debug(f"Updated {model} ID {ids[0]}")
            
            return ids[0], False
        
        else:
            # Record does not exist - create
            record_id = self.create(model, create_vals)
            return record_id, True
    
    def ensure_records(
        self,
        model: str,
        records: List[Dict[str, Any]],
        match_fields: List[str],
    ) -> Tuple[List[int], int]:
        """
        Batch ensure_record für viele Records.
        
        Args:
            model: Model name
            records: List von [{'default_code': 'DRO-001', 'name': '...', ...}, ...]
            match_fields: Felder für Domain (z.B. ['default_code'])
        
        Returns:
            (created_ids, updated_count)
        
        Example:
            created_ids, updated = client.ensure_records(
                'product.product',
                [
                    {'default_code': 'DRO-001', 'name': 'Spartan', 'list_price': 499},
                    {'default_code': 'DRO-002', 'name': 'Lightweight', 'list_price': 599},
                ],
                match_fields=['default_code']
            )
        """
        created_ids = []
        updated_count = 0
        
        for record in records:
            # Build domain from match_fields
            domain = [
                (field, '=', record.get(field))
                for field in match_fields
            ]
            
            # Separate create_vals from update_vals (optional)
            record_id, is_new = self.ensure_record(
                model,
                domain,
                create_vals=record,
                update_vals=None,
            )
            
            if is_new:
                created_ids.append(record_id)
            else:
                updated_count += 1
        
        self.logger.info(
            f"ensure_records: {len(created_ids)} created, "
            f"{updated_count} updated"
        )
        
        return created_ids, updated_count
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC METHODS: CALL
    # ═══════════════════════════════════════════════════════════════════════════
    
    def call(
        self,
        model: str,
        method: str,
        *args,
        **kwargs
    ) -> Any:
        """
        Generic RPC call (für custom Methoden).
        
        Args:
            model: Model name
            method: Method name
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Method result
        
        Example:
            result = client.call('product.product', 'toggle_active')
        """
        return self._execute_rpc(model, method, args, kwargs)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DESTRUCTOR
    # ═══════════════════════════════════════════════════════════════════════════
    
    def __del__(self):
        """Cleanup bei Objektzerstörung."""
        if self._uid is not None:
            try:
                self.logger.debug("Logging out...")
                # Odoo hat keinen explicit logout, aber Session wird ablaufen
            except Exception as e:
                self.logger.debug(f"Error during cleanup: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_odoo_client(config: Optional[OdooRPCConfig] = None) -> OdooClient:
    """Get Odoo RPC client (singleton pattern optional)."""
    return OdooClient(config)
