#!/usr/bin/env python3
"""
Ultimate File Organizer - Combined file organization, deduplication, and cleanup system
Combines organize_system.py, remove_duplicates.py, and cleanup_system.py into one unified solution
"""

import os
import shutil
import json
import time
import hashlib
import logging
import tempfile
import psutil
import signal
import threading
from typing import Dict, List, Any, Optional, Tuple, Callable
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import mimetypes
import re

logger = logging.getLogger(__name__)

# ============================================================================
# FILE ORGANIZATION COMPONENT (from organize_system.py)
# ============================================================================

class FileOrganizer:
    """Advanced file organization and categorization system"""
    
    def __init__(self, base_directory: str = None):
        self.base_directory = Path(base_directory) if base_directory else Path.cwd()
        self.organization_rules = []
        self.file_categories = self._default_categories()
        self.organization_history = []
        self.stats = {
            "files_organized": 0,
            "directories_created": 0,
            "space_organized": 0,
            "operations_completed": 0
        }
    
    def add_organization_rule(self, rule: Dict):
        """Add a file organization rule"""
        required_fields = ["name", "pattern", "destination", "condition"]
        for field in required_fields:
            if field not in rule:
                raise ValueError(f"Missing required field: {field}")
        
        self.organization_rules.append(rule)
        logger.info(f"📋 Added organization rule: {rule['name']}")
    
    def organize_by_type(self, source_dir: str = None, dry_run: bool = False) -> Dict[str, Any]:
        """Organize files by type into categorized directories"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "categories_created": set(),
            "errors": []
        }
        
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    category = self._categorize_file(file_path)
                    if category:
                        destination_dir = source / category
                        destination_file = destination_dir / file_path.name
                        
                        if not dry_run:
                            # Create category directory if needed
                            destination_dir.mkdir(exist_ok=True)
                            results["categories_created"].add(category)
                            
                            # Move file
                            shutil.move(str(file_path), str(destination_file))
                        
                        results["files_processed"] += 1
                        results["files_moved"] += 1
                        
                        logger.debug(f"📁 Organized {file_path.name} to {category}")
                
                except Exception as e:
                    error_msg = f"Error organizing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        results["categories_created"] = list(results["categories_created"])
        self._record_organization("type_organization", results)
        
        return results
    
    def organize_by_date(self, source_dir: str = None, date_format: str = "%Y/%m", dry_run: bool = False) -> Dict[str, Any]:
        """Organize files by modification date"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "date_folders_created": set(),
            "errors": []
        }
        
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    date_folder = mod_time.strftime(date_format)
                    
                    destination_dir = source / date_folder
                    destination_file = destination_dir / file_path.name
                    
                    if not dry_run:
                        destination_dir.mkdir(exist_ok=True)
                        results["date_folders_created"].add(date_folder)
                        
                        # Handle name conflicts
                        if destination_file.exists():
                            counter = 1
                            stem = destination_file.stem
                            suffix = destination_file.suffix
                            while destination_file.exists():
                                destination_file = destination_dir / f"{stem}_{counter}{suffix}"
                                counter += 1
                        
                        shutil.move(str(file_path), str(destination_file))
                    
                    results["files_processed"] += 1
                    results["files_moved"] += 1
                
                except Exception as e:
                    error_msg = f"Error organizing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        results["date_folders_created"] = list(results["date_folders_created"])
        self._record_organization("date_organization", results)
        
        return results
    
    def _categorize_file(self, file_path: Path) -> Optional[str]:
        """Categorize file by type"""
        # Check extension first
        ext = file_path.suffix.lower()
        
        for category, extensions in self.file_categories.items():
            if ext in extensions:
                return category
        
        # Use MIME type as fallback
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            if mime_type.startswith('image/'):
                return 'images'
            elif mime_type.startswith('video/'):
                return 'videos'
            elif mime_type.startswith('audio/'):
                return 'audio'
            elif mime_type.startswith('text/'):
                return 'documents'
            elif mime_type.startswith('application/'):
                if 'pdf' in mime_type:
                    return 'documents'
                elif 'zip' in mime_type or 'rar' in mime_type:
                    return 'archives'
                else:
                    return 'applications'
        
        return 'others'
    
    def _default_categories(self) -> Dict[str, List[str]]:
        """Default file categories"""
        return {
            'images': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.webp', '.heic'],
            'documents': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx'],
            'videos': ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'],
            'audio': ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma'],
            'archives': ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'],
            'code': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.php', '.rb', '.go'],
            'applications': ['.exe', '.msi', '.dmg', '.app', '.deb', '.rpm'],
            'others': []
        }
    
    def _record_organization(self, operation: str, results: Dict):
        """Record organization operation in history"""
        self.organization_history.append({
            "operation": operation,
            "results": results,
            "timestamp": time.time()
        })
        
        # Keep only last 1000 records
        if len(self.organization_history) > 1000:
            self.organization_history = self.organization_history[-1000:]

# ============================================================================
# DUPLICATE REMOVAL COMPONENT (from remove_duplicates.py)
# ============================================================================

class FileDuplicateRemover:
    """Advanced duplicate removal system with multiple strategies"""
    
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.removed_count = 0
        self.duplicate_sets = {}
    
    def scan_directory(self) -> Dict[str, List[str]]:
        """Scan directory for duplicate files using hash comparison"""
        file_hashes = defaultdict(list)
        
        print(f"🔍 Scanning {self.directory} for duplicates...")
        
        for file_path in self.directory.rglob("*"):
            if file_path.is_file():
                try:
                    # Calculate file hash
                    file_hash = self._calculate_file_hash(file_path)
                    if file_hash:
                        file_hashes[file_hash].append(str(file_path))
                except Exception as e:
                    logger.warning(f"⚠️  Could not hash {file_path}: {e}")
        
        # Filter to only include duplicates (hashes with multiple files)
        duplicates = {hash_val: paths for hash_val, paths in file_hashes.items() if len(paths) > 1}
        self.duplicate_sets = duplicates
        
        print(f"📊 Found {len(duplicates)} duplicate sets")
        total_duplicates = sum(len(paths)-1 for paths in duplicates.values())
        print(f"🔄 Total duplicate files: {total_duplicates}")
        
        return duplicates
    
    def remove_duplicates(self, strategy: str = "move") -> Dict[str, Any]:
        """Remove duplicate files using specified strategy"""
        if not self.duplicate_sets:
            self.scan_directory()
        
        results = {
            "duplicate_sets_found": len(self.duplicate_sets),
            "duplicates_removed": 0,
            "space_saved": 0,
            "errors": []
        }
        
        # Create duplicates directory
        duplicates_dir = self.directory / "_duplicates"
        if not duplicates_dir.exists():
            duplicates_dir.mkdir(exist_ok=True)
        
        for hash_val, file_paths in self.duplicate_sets.items():
            try:
                # Keep first file, move others to duplicates folder
                files_to_move = file_paths[1:]
                
                for file_path in files_to_move:
                    source_file = Path(file_path)
                    dest_file = duplicates_dir / source_file.name
                    
                    # Handle name conflicts
                    counter = 1
                    while dest_file.exists():
                        stem = dest_file.stem
                        suffix = dest_file.suffix
                        dest_file = duplicates_dir / f"{stem}_{counter}{suffix}"
                        counter += 1
                    
                    if strategy == "move":
                        shutil.move(str(source_file), str(dest_file))
                    elif strategy == "delete":
                        source_file.unlink()
                    
                    results["duplicates_removed"] += 1
                    results["space_saved"] += source_file.stat().st_size
            
            except Exception as e:
                error_msg = f"Error processing duplicate set {hash_val}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        print(f"✅ Removed {results['duplicates_removed']} duplicates")
        print(f"💾 Space saved: {results['space_saved']/(1024*1024):.1f} MB")
        
        return results
    
    def _calculate_file_hash(self, file_path: Path) -> Optional[str]:
        """Calculate SHA256 hash of file"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to hash file {file_path}: {e}")
            return None

# ============================================================================
# CLEANUP SYSTEM COMPONENT (from cleanup_system.py)
# ============================================================================

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
    
    def apply_cleanup_rules(self, target_dir: str = "/") -> Dict[str, Any]:
        """Apply all registered cleanup rules"""
        results = {}
        target_path = Path(target_dir)
        
        for rule in self.cleanup_rules:
            rule_name = rule["name"]
            pattern = rule["pattern"]
            action = rule["action"]
            condition = rule.get("condition", {})
            
            try:
                rule_results = self._apply_rule(target_path, rule)
                results[rule_name] = rule_results
            except Exception as e:
                logger.error(f"Error applying rule {rule_name}: {e}")
                results[rule_name] = {"success": False, "error": str(e)}
        
        return results
    
    def _apply_rule(self, target_path: Path, rule: Dict) -> Dict[str, Any]:
        """Apply a single cleanup rule"""
        pattern = rule["pattern"]
        action = rule["action"]
        condition = rule.get("condition", {})
        
        matched_items = []
        processed_items = []
        space_freed = 0
        
        # Find matching files
        for file_path in target_path.rglob(pattern):
            if file_path.is_file() and self._matches_condition(file_path, condition):
                matched_items.append(str(file_path))
                
                if action == "delete":
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        processed_items.append(str(file_path))
                        space_freed += file_size
                    except Exception as e:
                        logger.warning(f"Could not delete {file_path}: {e}")
        
        return {
            "success": True,
            "matched_items": len(matched_items),
            "processed_items": processed_items,
            "space_freed": space_freed
        }
    
    def _matches_condition(self, file_path: Path, condition: Dict) -> bool:
        """Check if file matches cleanup conditions"""
        if "age_min" in condition:
            file_age = time.time() - file_path.stat().st_mtime
            if file_age < condition["age_min"]:
                return False
        
        if "size_min" in condition:
            if file_path.stat().st_size < condition["size_min"]:
                return False
        
        if "size_max" in condition:
            if file_path.stat().st_size > condition["size_max"]:
                return False
        
        return True

# ============================================================================
# ULTIMATE ORGANIZER - Main Integration Class
# ============================================================================

class UltimateFileOrganizer:
    """Ultimate file organization system combining all three components"""
    
    def __init__(self, base_directory: str = None):
        self.base_directory = Path(base_directory) if base_directory else Path.cwd()
        self.file_organizer = FileOrganizer(str(self.base_directory))
        self.duplicate_remover = FileDuplicateRemover(str(self.base_directory))
        self.cleanup_manager = FileSystemCleanupManager()
        
        # Setup default cleanup rules
        self._setup_default_cleanup_rules()
        
        self.operation_history = []
    
    def _setup_default_cleanup_rules(self):
        """Setup default cleanup rules"""
        self.cleanup_manager.add_cleanup_rule({
            'name': 'clean_temp_files',
            'pattern': '*.tmp',
            'action': 'delete',
            'condition': {'age_min': 86400}  # older than 1 day
        })
        
        self.cleanup_manager.add_cleanup_rule({
            'name': 'clean_cache_files',
            'pattern': '*.cache',
            'action': 'delete',
            'condition': {'age_min': 86400}
        })
        
        self.cleanup_manager.add_cleanup_rule({
            'name': 'clean_log_files',
            'pattern': '*.log',
            'action': 'delete',
            'condition': {'age_min': 604800}  # older than 7 days
        })
    
    def organize_everything(self, config: Dict = None) -> Dict[str, Any]:
        """Run complete organization: organize, deduplicate, and cleanup"""
        if config is None:
            config = {
                "organize_by_type": True,
                "organize_by_date": False,
                "remove_duplicates": True,
                "cleanup_files": True,
                "dry_run": False
            }
        
        results = {
            "organization": {},
            "deduplication": {},
            "cleanup": {},
            "summary": {},
            "timestamp": time.time()
        }
        
        print("🚀 Starting Ultimate File Organization")
        print("=" * 50)
        
        # 1. FILE ORGANIZATION
        if config.get("organize_by_type", True):
            print("\n📁 1. Organizing files by type...")
            start_time = time.time()
            
            org_results = self.file_organizer.organize_by_type(
                dry_run=config.get("dry_run", False)
            )
            results["organization"]["by_type"] = org_results
            
            org_time = time.time() - start_time
            print(f"   ✅ {org_results['files_processed']} files processed")
            print(f"   📂 Categories: {org_results['categories_created']}")
            print(f"   ⏱️  Time: {org_time:.1f}s")
        
        if config.get("organize_by_date", False):
            print("\n📅 2. Organizing files by date...")
            start_time = time.time()
            
            date_results = self.file_organizer.organize_by_date(
                dry_run=config.get("dry_run", False)
            )
            results["organization"]["by_date"] = date_results
            
            date_time = time.time() - start_time
            print(f"   ✅ {date_results['files_processed']} files processed")
            print(f"   📅 Date folders: {date_results['date_folders_created']}")
            print(f"   ⏱️  Time: {date_time:.1f}s")
        
        # 2. DUPLICATE REMOVAL
        if config.get("remove_duplicates", True):
            print("\n🗑️  2. Removing duplicates...")
            start_time = time.time()
            
            dup_results = self.duplicate_remover.remove_duplicates(
                strategy=config.get("deduplication_strategy", "move")
            )
            results["deduplication"] = dup_results
            
            dup_time = time.time() - start_time
            print(f"   ✅ {dup_results['duplicates_removed']} duplicates removed")
            print(f"   💾 Space saved: {dup_results['space_saved']/(1024*1024):.1f} MB")
            print(f"   ⏱️  Time: {dup_time:.1f}s")
        
        # 3. SYSTEM CLEANUP
        if config.get("cleanup_files", True):
            print("\n🧹 3. Running system cleanup...")
            start_time = time.time()
            
            cleanup_results = self.cleanup_manager.apply_cleanup_rules(
                target_dir=str(self.base_directory)
            )
            results["cleanup"] = cleanup_results
            
            cleanup_time = time.time() - start_time
            total_cleaned = sum(
                result.get("matched_items", 0) 
                for result in cleanup_results.values() 
                if result.get("success")
            )
            total_space_saved = sum(
                result.get("space_freed", 0) 
                for result in cleanup_results.values() 
                if result.get("success")
            )
            
            print(f"   ✅ {total_cleaned} files cleaned")
            print(f"   💾 Space saved: {total_space_saved/(1024*1024):.1f} MB")
            print(f"   ⏱️  Time: {cleanup_time:.1f}s")
        
        # 4. SUMMARY
        print("\n" + "=" * 50)
        print("📊 ULTIMATE ORGANIZATION SUMMARY")
        print("=" * 50)
        
        # Calculate totals
        total_files_processed = 0
        total_categories_created = set()
        total_duplicates_removed = 0
        total_space_saved = 0
        total_files_cleaned = 0
        
        # Organization totals
        for org_type, org_results in results["organization"].items():
            if "files_processed" in org_results:
                total_files_processed += org_results["files_processed"]
            if "categories_created" in org_results:
                total_categories_created.update(org_results["categories_created"])
        
        # Deduplication totals
        if "duplicates_removed" in results["deduplication"]:
            total_duplicates_removed += results["deduplication"]["duplicates_removed"]
            total_space_saved += results["deduplication"]["space_saved"]
        
        # Cleanup totals
        for rule, cleanup_result in results["cleanup"].items():
            if isinstance(cleanup_result, dict) and cleanup_result.get("success"):
                total_files_cleaned += cleanup_result.get("matched_items", 0)
                total_space_saved += cleanup_result.get("space_freed", 0)
        
        # Create summary
        summary = {
            "total_files_processed": total_files_processed,
            "categories_created": list(total_categories_created),
            "duplicates_removed": total_duplicates_removed,
            "files_cleaned": total_files_cleaned,
            "total_space_saved": total_space_saved,
            "operations_completed": len([op for op in [config.get("organize_by_type"), config.get("organize_by_date"), 
                                                   config.get("remove_duplicates"), config.get("cleanup_files")] if op])
        }
        
        results["summary"] = summary
        
        print(f"📁 Files Organized: {total_files_processed:,}")
        print(f"📂 Categories Created: {len(total_categories_created)}")
        print(f"🗑️  Duplicates Removed: {total_duplicates_removed:,}")
        print(f"🧹 Files Cleaned: {total_files_cleaned:,}")
        print(f"💾 Total Space Saved: {total_space_saved/(1024*1024):.1f} MB")
        print(f"⚙️  Operations Completed: {summary['operations_completed']}")
        
        # Record operation
        self.operation_history.append(results)
        
        print(f"\n✅ Ultimate organization complete!")
        return results
    
    def get_operation_history(self, limit: int = 10) -> List[Dict]:
        """Get recent operation history"""
        return self.operation_history[-limit:]
    
    def export_results(self, results: Dict, filename: str = None) -> str:
        """Export results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ultimate_organization_results_{timestamp}.json"
        
        output_file = self.base_directory / filename
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"💾 Results exported to: {output_file}")
        return str(output_file)

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Command line interface for ultimate file organizer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ultimate File Organizer - Complete file management solution")
    parser.add_argument("directory", nargs="?", default=".", help="Directory to organize (default: current)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--no-organize", action="store_true", help="Skip file organization")
    parser.add_argument("--no-deduplicate", action="store_true", help="Skip duplicate removal")
    parser.add_argument("--no-cleanup", action="store_true", help="Skip system cleanup")
    parser.add_argument("--organize-by-date", action="store_true", help="Also organize by date")
    parser.add_argument("--export", help="Export results to file")
    
    args = parser.parse_args()
    
    # Create configuration
    config = {
        "organize_by_type": not args.no_organize,
        "organize_by_date": args.organize_by_date,
        "remove_duplicates": not args.no_deduplicate,
        "cleanup_files": not args.no_cleanup,
        "dry_run": args.dry_run
    }
    
    # Run ultimate organizer
    organizer = UltimateFileOrganizer(args.directory)
    results = organizer.organize_everything(config)
    
    # Export results if requested
    if args.export:
        organizer.export_results(results, args.export)
    
    return results

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run main function
    main()
