#!/usr/bin/env python3
"""
Advanced Cleanup System - Comprehensive cleanup and maintenance utilities
"""

import os
import time
import threading
import logging
import signal
import psutil
import shutil
import tempfile
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

class ProcessCleanupManager:
    """Advanced process cleanup and management system"""
    
    def __init__(self):
        self.active_processes = {}  # pid -> process info
        self.process_lock = threading.Lock()
        self.cleanup_strategies = {
            'graceful': self._graceful_cleanup,
            'force': self._force_cleanup,
            'timeout': self._timeout_cleanup
        }
        self.cleanup_history = []
    
    def register_process(self, pid: int, command: str, process_obj=None, metadata: Dict = None):
        """Register a process for monitoring and cleanup"""
        with self.process_lock:
            self.active_processes[pid] = {
                "pid": pid,
                "command": command,
                "process": process_obj,
                "start_time": time.time(),
                "status": "running",
                "metadata": metadata or {},
                "cleanup_attempts": 0
            }
            logger.info(f"🆔 Registered process {pid}: {command[:50]}...")
    
    def cleanup_process(self, pid: int, strategy: str = 'graceful', timeout: int = 30) -> bool:
        """Cleanup a specific process using specified strategy"""
        if strategy not in self.cleanup_strategies:
            raise ValueError(f"Unknown cleanup strategy: {strategy}")
        
        with self.process_lock:
            if pid not in self.active_processes:
                logger.warning(f"⚠️  Process {pid} not found in registry")
                return False
            
            process_info = self.active_processes[pid]
            process_info["cleanup_attempts"] += 1
            
            try:
                success = self.cleanup_strategies[strategy](pid, timeout)
                
                if success:
                    self._record_cleanup(pid, strategy, success)
                    del self.active_processes[pid]
                    logger.info(f"✅ Successfully cleaned up process {pid}")
                else:
                    logger.warning(f"⚠️  Failed to cleanup process {pid} with {strategy} strategy")
                
                return success
                
            except Exception as e:
                logger.error(f"💥 Error cleaning up process {pid}: {e}")
                return False
    
    def cleanup_all_processes(self, strategy: str = 'graceful') -> Dict[int, bool]:
        """Cleanup all registered processes"""
        results = {}
        
        with self.process_lock:
            pids = list(self.active_processes.keys())
        
        for pid in pids:
            results[pid] = self.cleanup_process(pid, strategy)
        
        return results
    
    def _graceful_cleanup(self, pid: int, timeout: int) -> bool:
        """Graceful process termination using SIGTERM"""
        try:
            if psutil.pid_exists(pid):
                process = psutil.Process(pid)
                process.terminate()
                
                # Wait for graceful termination
                try:
                    process.wait(timeout=timeout)
                    return True
                except psutil.TimeoutExpired:
                    # If timeout, try force cleanup
                    return self._force_cleanup(pid, 5)
            
            return True  # Process already terminated
            
        except psutil.NoSuchProcess:
            return True
        except Exception as e:
            logger.error(f"💥 Graceful cleanup failed for {pid}: {e}")
            return False
    
    def _force_cleanup(self, pid: int, timeout: int) -> bool:
        """Force process termination using SIGKILL"""
        try:
            if psutil.pid_exists(pid):
                process = psutil.Process(pid)
                process.kill()
                
                try:
                    process.wait(timeout=timeout)
                    return True
                except psutil.TimeoutExpired:
                    return False
            
            return True
            
        except psutil.NoSuchProcess:
            return True
        except Exception as e:
            logger.error(f"💥 Force cleanup failed for {pid}: {e}")
            return False
    
    def _timeout_cleanup(self, pid: int, timeout: int) -> bool:
        """Cleanup with timeout - try graceful first, then force"""
        if not self._graceful_cleanup(pid, min(timeout, 10)):
            return self._force_cleanup(pid, 5)
        return True
    
    def _record_cleanup(self, pid: int, strategy: str, success: bool):
        """Record cleanup operation in history"""
        self.cleanup_history.append({
            "pid": pid,
            "strategy": strategy,
            "success": success,
            "timestamp": time.time(),
            "command": self.active_processes.get(pid, {}).get("command", "Unknown")
        })
        
        # Keep only last 1000 cleanup records
        if len(self.cleanup_history) > 1000:
            self.cleanup_history = self.cleanup_history[-1000:]
    
    def get_process_status(self, pid: int) -> Optional[Dict]:
        """Get status of a registered process"""
        with self.process_lock:
            return self.active_processes.get(pid)
    
    def list_active_processes(self) -> Dict[int, Dict]:
        """List all active processes"""
        with self.process_lock:
            return dict(self.active_processes)
    
    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get cleanup statistics"""
        total_cleanups = len(self.cleanup_history)
        successful_cleanups = sum(1 for c in self.cleanup_history if c["success"])
        
        strategy_stats = defaultdict(int)
        for cleanup in self.cleanup_history:
            strategy_stats[cleanup["strategy"]] += 1
        
        return {
            "total_cleanups": total_cleanups,
            "successful_cleanups": successful_cleanups,
            "success_rate": (successful_cleanups / total_cleanups * 100) if total_cleanups > 0 else 0,
            "active_processes": len(self.active_processes),
            "strategy_usage": dict(strategy_stats)
        }

class CacheCleanupManager:
    """Advanced cache cleanup and management system"""
    
    def __init__(self, cleanup_interval: int = 300):  # 5 minutes default
        self.cleanup_interval = cleanup_interval
        self.cache_stores = {}
        self.cleanup_thread = None
        self.running = False
        self.cleanup_stats = {
            "total_cleanups": 0,
            "items_removed": 0,
            "space_freed": 0
        }
    
    def register_cache(self, name: str, cache_store: Any, cleanup_policy: Dict = None):
        """Register a cache store for cleanup"""
        self.cache_stores[name] = {
            "store": cache_store,
            "policy": cleanup_policy or self._default_cleanup_policy(),
            "last_cleanup": time.time()
        }
        logger.info(f"📋 Registered cache store: {name}")
    
    def start_auto_cleanup(self):
        """Start automatic cleanup thread"""
        if self.running:
            logger.warning("⚠️  Auto cleanup already running")
            return
        
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._auto_cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info(f"🔄 Started auto cleanup (interval: {self.cleanup_interval}s)")
    
    def stop_auto_cleanup(self):
        """Stop automatic cleanup thread"""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        logger.info("⏹️  Stopped auto cleanup")
    
    def cleanup_cache(self, cache_name: str = None) -> Dict[str, Any]:
        """Manually cleanup specified cache or all caches"""
        results = {}
        
        caches_to_clean = [cache_name] if cache_name else list(self.cache_stores.keys())
        
        for name in caches_to_clean:
            if name in self.cache_stores:
                result = self._cleanup_single_cache(name)
                results[name] = result
            else:
                logger.warning(f"⚠️  Cache store {name} not found")
                results[name] = {"success": False, "error": "Cache not found"}
        
        return results
    
    def _auto_cleanup_loop(self):
        """Background cleanup loop"""
        while self.running:
            try:
                self.cleanup_cache()
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"💥 Auto cleanup error: {e}")
                time.sleep(60)  # Wait before retry
    
    def _cleanup_single_cache(self, cache_name: str) -> Dict[str, Any]:
        """Cleanup a single cache store"""
        cache_info = self.cache_stores[cache_name]
        cache_store = cache_info["store"]
        policy = cache_info["policy"]
        
        try:
            items_before = self._get_cache_size(cache_store)
            space_before = self._get_cache_memory_usage(cache_store)
            
            # Apply cleanup policy
            items_removed = self._apply_cleanup_policy(cache_store, policy)
            
            items_after = self._get_cache_size(cache_store)
            space_after = self._get_cache_memory_usage(cache_store)
            
            # Update stats
            actual_items_removed = items_before - items_after
            space_freed = space_before - space_after
            
            self.cleanup_stats["total_cleanups"] += 1
            self.cleanup_stats["items_removed"] += actual_items_removed
            self.cleanup_stats["space_freed"] += space_freed
            
            cache_info["last_cleanup"] = time.time()
            
            logger.info(f"🧹 Cleaned cache {cache_name}: removed {actual_items_removed} items, freed {space_freed} bytes")
            
            return {
                "success": True,
                "items_before": items_before,
                "items_after": items_after,
                "items_removed": actual_items_removed,
                "space_freed": space_freed
            }
            
        except Exception as e:
            logger.error(f"💥 Cache cleanup failed for {cache_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def _apply_cleanup_policy(self, cache_store: Any, policy: Dict) -> int:
        """Apply cleanup policy to cache store"""
        items_removed = 0
        
        # Remove expired items
        if policy.get("remove_expired", True) and hasattr(cache_store, "remove_expired"):
            items_removed += cache_store.remove_expired()
        
        # Remove items older than max_age
        if policy.get("max_age"):
            items_removed += self._remove_by_age(cache_store, policy["max_age"])
        
        # Remove least recently used items if over max_size
        if policy.get("max_size") and hasattr(cache_store, "__len__"):
            current_size = len(cache_store)
            if current_size > policy["max_size"]:
                items_to_remove = current_size - policy["max_size"]
                items_removed += self._remove_lru_items(cache_store, items_to_remove)
        
        return items_removed
    
    def _remove_by_age(self, cache_store: Any, max_age: int) -> int:
        """Remove items older than max_age seconds"""
        items_removed = 0
        current_time = time.time()
        
        if hasattr(cache_store, "items"):
            keys_to_remove = []
            for key, value in cache_store.items():
                if hasattr(value, "timestamp"):
                    if current_time - value.timestamp > max_age:
                        keys_to_remove.append(key)
                elif hasattr(value, "get") and "timestamp" in value:
                    if current_time - value["timestamp"] > max_age:
                        keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del cache_store[key]
                items_removed += 1
        
        return items_removed
    
    def _remove_lru_items(self, cache_store: Any, count: int) -> int:
        """Remove least recently used items"""
        items_removed = 0
        
        if hasattr(cache_store, "items") and hasattr(cache_store, "get_access_time"):
            # Sort by access time and remove oldest
            items_by_access = sorted(
                cache_store.items(),
                key=lambda x: cache_store.get_access_time(x[0])
            )
            
            for key, _ in items_by_access[:count]:
                del cache_store[key]
                items_removed += 1
        
        return items_removed
    
    def _get_cache_size(self, cache_store: Any) -> int:
        """Get cache size"""
        return len(cache_store) if hasattr(cache_store, "__len__") else 0
    
    def _get_cache_memory_usage(self, cache_store: Any) -> int:
        """Estimate cache memory usage"""
        try:
            import sys
            return sys.getsizeof(cache_store)
        except:
            return 0
    
    def _default_cleanup_policy(self) -> Dict:
        """Default cleanup policy"""
        return {
            "remove_expired": True,
            "max_age": 3600,  # 1 hour
            "max_size": 1000
        }
    
    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get cleanup statistics"""
        return dict(self.cleanup_stats)

class FileSystemCleanupManager:
    """File system cleanup and maintenance utilities"""
    
    def __init__(self):
        self.cleanup_rules = []
        self.cleanup_history = []
    
    def add_cleanup_rule(self, rule: Dict):
        """Add a cleanup rule"""
        required_fields = ["name", "pattern", "action", "condition"]
        for field in required_fields:
            if field not in rule:
                raise ValueError(f"Missing required field: {field}")
        
        self.cleanup_rules.append(rule)
        logger.info(f"📋 Added cleanup rule: {rule['name']}")
    
    def cleanup_temp_files(self, temp_dirs: List[str] = None, max_age: int = 86400) -> Dict[str, Any]:
        """Clean up temporary files older than max_age seconds"""
        if temp_dirs is None:
            temp_dirs = [tempfile.gettempdir()]
        
        removed_files = []
        removed_dirs = []
        space_freed = 0
        
        for temp_dir in temp_dirs:
            if not os.path.exists(temp_dir):
                continue
            
            for item in Path(temp_dir).rglob("*"):
                try:
                    item_age = time.time() - item.stat().st_mtime
                    
                    if item_age > max_age:
                        item_size = item.stat().st_size if item.is_file() else 0
                        
                        if item.is_file():
                            item.unlink()
                            removed_files.append(str(item))
                        elif item.is_dir() and not any(item.iterdir()):
                            item.rmdir()
                            removed_dirs.append(str(item))
                        
                        space_freed += item_size
                        
                except Exception as e:
                    logger.warning(f"⚠️  Could not clean {item}: {e}")
        
        result = {
            "removed_files": removed_files,
            "removed_dirs": removed_dirs,
            "space_freed": space_freed,
            "total_removed": len(removed_files) + len(removed_dirs)
        }
        
        self._record_cleanup("temp_files", result)
        logger.info(f"🧹 Cleaned temp files: {result['total_removed']} items, freed {space_freed} bytes")
        
        return result
    
    def cleanup_log_files(self, log_dirs: List[str], max_age: int = 604800) -> Dict[str, Any]:  # 7 days
        """Clean up old log files"""
        removed_files = []
        space_freed = 0
        
        for log_dir in log_dirs:
            if not os.path.exists(log_dir):
                continue
            
            for log_file in Path(log_dir).rglob("*.log"):
                try:
                    file_age = time.time() - log_file.stat().st_mtime
                    
                    if file_age > max_age:
                        file_size = log_file.stat().st_size
                        log_file.unlink()
                        
                        removed_files.append(str(log_file))
                        space_freed += file_size
                        
                except Exception as e:
                    logger.warning(f"⚠️  Could not clean log file {log_file}: {e}")
        
        result = {
            "removed_files": removed_files,
            "space_freed": space_freed,
            "total_removed": len(removed_files)
        }
        
        self._record_cleanup("log_files", result)
        logger.info(f"🧹 Cleaned log files: {result['total_removed']} files, freed {space_freed} bytes")
        
        return result
    
    def cleanup_empty_directories(self, root_dir: str) -> Dict[str, Any]:
        """Remove empty directories recursively"""
        removed_dirs = []
        
        for root, dirs, files in os.walk(root_dir, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                
                try:
                    if not os.listdir(dir_path):  # Directory is empty
                        os.rmdir(dir_path)
                        removed_dirs.append(dir_path)
                except Exception as e:
                    logger.warning(f"⚠️  Could not remove empty directory {dir_path}: {e}")
        
        result = {
            "removed_dirs": removed_dirs,
            "total_removed": len(removed_dirs)
        }
        
        self._record_cleanup("empty_dirs", result)
        logger.info(f"🧹 Cleaned empty directories: {result['total_removed']} directories")
        
        return result
    
    def apply_cleanup_rules(self, target_dir: str = "/") -> Dict[str, Any]:
        """Apply all registered cleanup rules"""
        results = {}
        
        for rule in self.cleanup_rules:
            try:
                result = self._apply_single_rule(rule, target_dir)
                results[rule["name"]] = result
            except Exception as e:
                logger.error(f"💥 Failed to apply rule {rule['name']}: {e}")
                results[rule["name"]] = {"success": False, "error": str(e)}
        
        return results
    
    def _apply_single_rule(self, rule: Dict, target_dir: str) -> Dict[str, Any]:
        """Apply a single cleanup rule"""
        pattern = rule["pattern"]
        action = rule["action"]
        condition = rule["condition"]
        
        matched_items = []
        processed_items = []
        space_freed = 0
        
        # Find matching items
        for item in Path(target_dir).rglob(pattern):
            try:
                if self._evaluate_condition(item, condition):
                    matched_items.append(item)
                    
                    # Apply action
                    if action == "delete":
                        item_size = item.stat().st_size if item.is_file() else 0
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
                        
                        processed_items.append(str(item))
                        space_freed += item_size
                    
                    elif action == "move" and "destination" in rule:
                        destination = Path(rule["destination"]) / item.name
                        shutil.move(str(item), str(destination))
                        processed_items.append(str(item))
                    
            except Exception as e:
                logger.warning(f"⚠️  Could not process {item}: {e}")
        
        result = {
            "success": True,
            "matched_items": len(matched_items),
            "processed_items": processed_items,
            "space_freed": space_freed
        }
        
        self._record_cleanup(rule["name"], result)
        return result
    
    def _evaluate_condition(self, item: Path, condition: Dict) -> bool:
        """Evaluate cleanup condition for an item"""
        try:
            # Age condition
            if "max_age" in condition:
                item_age = time.time() - item.stat().st_mtime
                if item_age < condition["max_age"]:
                    return False
            
            # Size condition
            if "max_size" in condition and item.is_file():
                if item.stat().st_size > condition["max_size"]:
                    return False
            
            # Empty directory condition
            if "empty" in condition and condition["empty"] and item.is_dir():
                if any(item.iterdir()):
                    return False
            
            return True
            
        except:
            return False
    
    def _record_cleanup(self, operation: str, result: Dict):
        """Record cleanup operation in history"""
        self.cleanup_history.append({
            "operation": operation,
            "result": result,
            "timestamp": time.time()
        })
        
        # Keep only last 1000 records
        if len(self.cleanup_history) > 1000:
            self.cleanup_history = self.cleanup_history[-1000:]
    
    def get_cleanup_history(self, limit: int = 100) -> List[Dict]:
        """Get cleanup history"""
        return self.cleanup_history[-limit:]

class IntegratedCleanupSystem:
    """Integrated cleanup system combining all cleanup managers"""
    
    def __init__(self):
        self.process_manager = ProcessCleanupManager()
        self.cache_manager = CacheCleanupManager()
        self.fs_manager = FileSystemCleanupManager()
        self.running = False
        self.monitor_thread = None
    
    def start_monitoring(self, interval: int = 300):
        """Start integrated cleanup monitoring"""
        if self.running:
            logger.warning("⚠️  Monitoring already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"🔄 Started integrated cleanup monitoring (interval: {interval}s)")
    
    def stop_monitoring(self):
        """Stop integrated cleanup monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        self.cache_manager.stop_auto_cleanup()
        logger.info("⏹️  Stopped integrated cleanup monitoring")
    
    def _monitoring_loop(self, interval: int):
        """Background monitoring loop"""
        while self.running:
            try:
                # Check for zombie processes
                self._cleanup_zombie_processes()
                
                # Auto cleanup caches
                self.cache_manager.cleanup_cache()
                
                # Periodic filesystem cleanup
                if int(time.time()) % 3600 < interval:  # Approximately every hour
                    self._periodic_fs_cleanup()
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"💥 Monitoring loop error: {e}")
                time.sleep(60)
    
    def _cleanup_zombie_processes(self):
        """Clean up zombie processes"""
        active_processes = self.process_manager.list_active_processes()
        
        for pid, process_info in active_processes.items():
            if not psutil.pid_exists(pid):
                logger.info(f"🧹 Cleaning up zombie process {pid}")
                self.process_manager.cleanup_process(pid, strategy='force')
    
    def _periodic_fs_cleanup(self):
        """Periodic filesystem cleanup"""
        try:
            # Clean temp files
            self.fs_manager.cleanup_temp_files(max_age=86400)  # 24 hours
            
            # Clean empty directories
            self.fs_manager.cleanup_empty_directories("/tmp")
            
        except Exception as e:
            logger.error(f"💥 Periodic FS cleanup error: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system cleanup status"""
        return {
            "process_manager": self.process_manager.get_cleanup_stats(),
            "cache_manager": self.cache_manager.get_cleanup_stats(),
            "fs_manager": {
                "rules_count": len(self.fs_manager.cleanup_rules),
                "history_count": len(self.fs_manager.cleanup_history)
            },
            "monitoring_active": self.running
        }

if __name__ == "__main__":
    # Example usage
    cleanup_system = IntegratedCleanupSystem()
    
    # Start monitoring
    cleanup_system.start_monitoring()
    
    # Register a test process
    cleanup_system.process_manager.register_process(
        pid=12345,
        command="test_command",
        metadata={"type": "test"}
    )
    
    # Add cleanup rule
    cleanup_system.fs_manager.add_cleanup_rule({
        "name": "temp_cleanup",
        "pattern": "*.tmp",
        "action": "delete",
        "condition": {"max_age": 3600}
    })
    
    # Get status
    status = cleanup_system.get_system_status()
    print("System Status:", json.dumps(status, indent=2))
    
    # Stop monitoring
    cleanup_system.stop_monitoring()
