"""
mailserver_loader.py - Mail Server Configuration Loader

Konfiguriert:
- SMTP Outgoing Server (Odoo Mail versand)
- IMAP Incoming Server (Odoo Fetchmail)
- Mail Parameters (System-Einstellungen)
- Environment Variable Resolution
- Connection Testing

Sicherheit:
- Passwörter NICHT in Logs
- Environment Variables für Credentials
- Validierung aller Eingaben
"""

import os
import re
import logging
import smtplib
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from email.mime.text import MIMEText
from datetime import datetime

from provisioning.client import OdooClient, RecordAmbiguousError
from provisioning.config import MAIL_CONFIG
from provisioning.utils import log_header, log_success, log_info, log_warn, log_error


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════════════════

class MailConfigError(Exception):
    """Base exception for mail configuration."""
    pass


class MailValidationError(MailConfigError):
    """Mail data validation error."""
    pass


class MailEnvError(MailConfigError):
    """Environment variable resolution error."""
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATORS
# ═══════════════════════════════════════════════════════════════════════════════

class MailValidator:
    """Validate mail configuration."""
    
    @staticmethod
    def validate_email(email: str) -> str:
        """Validate email format."""
        if not email or not isinstance(email, str):
            raise MailValidationError("Email required")
        
        email = email.strip()
        
        # Simple email validation
        if '@' not in email or '.' not in email.split('@')[1]:
            raise MailValidationError(f"Invalid email format: {email}")
        
        if len(email) > 254:
            raise MailValidationError(f"Email too long: {email}")
        
        return email
    
    @staticmethod
    def validate_port(port: int, min_port: int = 1, max_port: int = 65535) -> int:
        """Validate port number."""
        if not isinstance(port, int):
            try:
                port = int(port)
            except (ValueError, TypeError):
                raise MailValidationError(f"Invalid port: {port}")
        
        if port < min_port or port > max_port:
            raise MailValidationError(f"Port out of range: {port}")
        
        return port
    
    @staticmethod
    def validate_hostname(hostname: str) -> str:
        """Validate hostname/IP."""
        if not hostname or not isinstance(hostname, str):
            raise MailValidationError("Hostname required")
        
        hostname = hostname.strip().lower()
        
        # Very basic validation
        if len(hostname) < 3:
            raise MailValidationError(f"Invalid hostname: {hostname}")
        
        return hostname


# ═══════════════════════════════════════════════════════════════════════════════
# ENV VAR RESOLVER
# ═══════════════════════════════════════════════════════════════════════════════

class EnvVarResolver:
    """Resolve environment variables in configuration."""
    
    # Pattern: ${VAR_NAME}
    ENV_PATTERN = r'\$\{([A-Z_0-9]+)\}'
    
    @staticmethod
    def resolve(value: Any, allow_missing: bool = False) -> Any:
        """
        Resolve environment variables in value.
        
        Args:
            value: Value that may contain ${VAR_NAME} patterns
            allow_missing: If True, keep unresolved vars; if False, raise error
        
        Returns:
            Value with variables resolved
        
        Raises:
            MailEnvError: If required variable not found
        """
        if not isinstance(value, str):
            return value
        
        def replace_var(match):
            var_name = match.group(1)
            var_value = os.getenv(var_name)
            
            if var_value is None:
                if allow_missing:
                    logger.warning(f"Environment variable not set: {var_name}")
                    return match.group(0)  # Keep original
                else:
                    raise MailEnvError(f"Required environment variable not set: {var_name}")
            
            return var_value
        
        try:
            return re.sub(EnvVarResolver.ENV_PATTERN, replace_var, value)
        except MailEnvError:
            raise
    
    @staticmethod
    def resolve_dict(config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all string values in dict."""
        resolved = {}
        
        for key, value in config.items():
            if isinstance(value, str):
                try:
                    resolved[key] = EnvVarResolver.resolve(value, allow_missing=False)
                except MailEnvError as e:
                    raise MailEnvError(f"In {key}: {e}")
            else:
                resolved[key] = value
        
        return resolved


# ═══════════════════════════════════════════════════════════════════════════════
# MAIL SERVER LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class MailServerLoader:
    """Load and configure mail servers."""
    
    def __init__(self, client: OdooClient, base_data_dir: str):
        self.client = client
        self.base_data_dir = Path(base_data_dir)
        
        # Statistics
        self.stats = {
            'smtp_servers_created': 0,
            'smtp_servers_updated': 0,
            'imap_servers_created': 0,
            'imap_servers_updated': 0,
            'mail_parameters_updated': 0,
            'connection_tests_passed': 0,
            'connection_tests_failed': 0,
            'errors': 0,
        }
        
        # Audit log
        self.audit_log: List[Dict[str, Any]] = []
        
        logger.info(f"MailServerLoader initialized")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SMTP
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _ensure_smtp_server(self, config: Dict[str, Any]) -> Optional[int]:
        """Create or update SMTP outgoing server."""
        try:
            # Validate required fields
            name = config.get('name', '').strip()
            if not name:
                raise MailValidationError("SMTP server name required")
            
            smtp_host = MailValidator.validate_hostname(config.get('smtp_host', ''))
            smtp_port = MailValidator.validate_port(config.get('smtp_port', 587))
            smtp_user = config.get('smtp_user', '').strip()
            
            if not smtp_user:
                raise MailValidationError("SMTP user required")
            
            # Validate email if from_email provided
            if 'smtp_from' in config:
                config['smtp_from'] = MailValidator.validate_email(config['smtp_from'])
            
            # Build vals
            vals = {
                'name': name,
                'smtp_host': smtp_host,
                'smtp_port': smtp_port,
                'smtp_user': smtp_user,
                'smtp_encryption': config.get('smtp_encryption', 'starttls'),
                'active': config.get('active', True),
            }
            
            # Add optional fields
            if 'smtp_pass' in config:
                vals['smtp_pass'] = config['smtp_pass']
            
            if 'smtp_from' in config:
                vals['smtp_from'] = config['smtp_from']
            
            # Ensure in Odoo
            domain = [('name', '=', name)]
            
            server_id, is_new = self.client.ensure_record(
                'ir.mail_server',
                domain,
                vals,
                vals,
            )
            
            if is_new:
                self.stats['smtp_servers_created'] += 1
                log_success(f"[SMTP:NEW] {name} → {server_id}")
            else:
                self.stats['smtp_servers_updated'] += 1
                log_info(f"[SMTP:UPD] {name} → {server_id}")
            
            # Test connection
            if config.get('test_connection', True):
                self._test_smtp_connection(server_id, name)
            
            self._audit_log({
                'action': 'created' if is_new else 'updated',
                'server_type': 'smtp',
                'server_name': name,
                'server_id': server_id,
            })
            
            return server_id
        
        except MailValidationError as e:
            logger.error(f"SMTP validation failed: {e}")
            self.stats['errors'] += 1
            return None
    
    def _test_smtp_connection(self, server_id: int, server_name: str) -> bool:
        """Test SMTP connection without exposing credentials."""
        try:
            # Use Odoo's built-in test method
            self.client.execute(
                'ir.mail_server',
                'test_connection',
                [server_id]
            )
            
            self.stats['connection_tests_passed'] += 1
            log_success(f"[SMTP:TEST] {server_name} connection OK")
            return True
        
        except Exception as e:
            # Log error WITHOUT credentials
            self.stats['connection_tests_failed'] += 1
            log_warn(f"[SMTP:TEST:FAIL] {server_name}: {type(e).__name__}")
            logger.debug(f"SMTP test details: {e}")
            return False
    
    # ═══════════════════════════════════════════════════════════════════════════
    # IMAP
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _ensure_imap_server(self, config: Dict[str, Any]) -> Optional[int]:
        """Create or update IMAP incoming server."""
        try:
            # Validate required fields
            name = config.get('name', '').strip()
            if not name:
                raise MailValidationError("IMAP server name required")
            
            server_host = MailValidator.validate_hostname(config.get('server', ''))
            server_port = MailValidator.validate_port(config.get('port', 993))
            imap_user = config.get('user', '').strip()
            
            if not imap_user:
                raise MailValidationError("IMAP user required")
            
            # Build vals
            vals = {
                'name': name,
                'server': server_host,
                'port': server_port,
                'user': imap_user,
                'is_ssl': config.get('is_ssl', True),
                'active': config.get('active', True),
            }
            
            # Add optional fields
            if 'password' in config:
                vals['password'] = config['password']
            
            if 'priority' in config:
                vals['priority'] = int(config['priority'])
            
            # Ensure in Odoo
            domain = [('name', '=', name)]
            
            server_id, is_new = self.client.ensure_record(
                'fetchmail.server',
                domain,
                vals,
                vals,
            )
            
            if is_new:
                self.stats['imap_servers_created'] += 1
                log_success(f"[IMAP:NEW] {name} → {server_id}")
            else:
                self.stats['imap_servers_updated'] += 1
                log_info(f"[IMAP:UPD] {name} → {server_id}")
            
            self._audit_log({
                'action': 'created' if is_new else 'updated',
                'server_type': 'imap',
                'server_name': name,
                'server_id': server_id,
            })
            
            return server_id
        
        except MailValidationError as e:
            logger.error(f"IMAP validation failed: {e}")
            self.stats['errors'] += 1
            return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIL PARAMETERS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _setup_mail_parameters(self, params: Dict[str, str]) -> None:
        """Setup mail system parameters."""
        if not params:
            return
        
        log_header("Setting up Mail Parameters")
        
        for key, value in params.items():
            try:
                domain = [('key', '=', key)]
                
                _, is_new = self.client.ensure_record(
                    'ir.config_parameter',
                    domain,
                    {'key': key, 'value': value},
                    {'value': value},
                )
                
                if is_new:
                    log_success(f"[PARAM:NEW] {key}")
                else:
                    log_info(f"[PARAM:UPD] {key}")
                
                self.stats['mail_parameters_updated'] += 1
            
            except Exception as e:
                logger.error(f"Failed to set parameter {key}: {e}")
                self.stats['errors'] += 1
    
    # ═══════════════════════════════════════════════════════════════════════════
    # AUDIT
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _audit_log(self, data: Dict[str, Any]) -> None:
        """Add audit entry."""
        data['timestamp'] = datetime.now().isoformat()
        self.audit_log.append(data)
    
    def _persist_audit_log(self) -> None:
        """Write audit log to file."""
        import json
        
        audit_path = self.base_data_dir / 'audit' / 'mail_audit.json'
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(audit_path, 'w') as f:
                json.dump(self.audit_log, f, indent=2, default=str)
            logger.info(f"Audit log: {audit_path}")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # MAIN
    # ═══════════════════════════════════════════════════════════════════════════
    
    def run(self) -> Dict[str, int]:
        """Main entry point."""
        try:
            log_header("MAIL SERVER LOADER")
            
            # Get mail config
            mail_params = getattr(MAIL_CONFIG, 'parameters', {})
            mail_servers = getattr(MAIL_CONFIG, 'servers', [])
            
            if not mail_servers and not mail_params:
                log_warn("No mail configuration found")
                return {'skipped': True}
            
            # Process servers
            for server_config in mail_servers:
                try:
                    # Resolve environment variables
                    config = EnvVarResolver.resolve_dict(server_config)
                    
                    # Determine server type
                    server_type = config.pop('type', '').lower()
                    
                    if server_type == 'smtp':
                        self._ensure_smtp_server(config)
                    elif server_type == 'imap':
                        self._ensure_imap_server(config)
                    else:
                        logger.warning(f"Unknown server type: {server_type}")
                
                except MailEnvError as e:
                    logger.error(f"Environment variable error: {e}")
                    self.stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Failed to load mail server: {e}", exc_info=True)
                    self.stats['errors'] += 1
            
            # Setup parameters
            if mail_params:
                try:
                    params = EnvVarResolver.resolve_dict(mail_params)
                    self._setup_mail_parameters(params)
                except Exception as e:
                    logger.error(f"Failed to setup mail parameters: {e}")
                    self.stats['errors'] += 1
            
            # Persist audit
            self._persist_audit_log()
            
            # Summary
            log_success("Mail server loader completed")
            log_info("Statistics:")
            for key, value in self.stats.items():
                log_info(f"  {key}: {value}")
            
            return self.stats
        
        except Exception as e:
            log_error(f"Mail server loader failed: {e}", exc_info=True)
            raise
